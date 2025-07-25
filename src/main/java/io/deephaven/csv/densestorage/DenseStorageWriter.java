package io.deephaven.csv.densestorage;

import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.util.MutableObject;
import io.deephaven.csv.util.Pair;

import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;

/**
 * The DenseStorageWriter and {@link DenseStorageReader} work in tandem, forming a FIFO queue. The DenseStorageWriter
 * writes data, and the {@link DenseStorageReader} reads that data. If the {@link DenseStorageReader} "catches up", it
 * will block until the DenseStorageWriter provides more data, or indicates that it is done (via the {@link #finish()}
 * method. This synchronization is done at "block" granularity, so the DenseStorageReader can only proceed when the
 * DenseStorageWriter has written at least a "block" of data or is done. We allow multiple independent
 * {@link DenseStorageReader}s to consume the same underlying data. In our implementation this is used so our type
 * inferencer can take a second "pass" over the same input data.
 *
 * <p>
 * The point of this object is to store a sequence of (character sequences aka "strings", but not java.lang.String),
 * using a small fraction of overhead. The problem with storing every character sequence as a java.lang.String is:
 *
 * <ol>
 * <li>Per-object overhead (probably 8 or 16 bytes depending on pointer width)
 * <li>The memory cost of holding a reference to that String (again 4 or 8 bytes)
 * <li>The string has to know its length (4 bytes)
 * <li>Java characters are 2 bytes even though in practice many strings are ASCII-only and their chars can fit in a
 * byte. (Newer Java implementations can store text as bytes, eliminating this objection)
 * </ol>
 *
 * <p>
 * For small strings (say the word "hello" or the input text "12345.6789") the overhead can be 100% or worse.
 *
 * <p>
 * For our purposes we:
 *
 * <ol>
 * <li>Only need sequential access. i.e. we don't need random access into the sequence of "strings". So we can support a
 * model where we can have a forward-only cursor moving over the sequence of "strings".
 * <li>Don't need to give our caller a data structure that they can hold on to. The caller only gets a "view" (a slice)
 * of the current "string" data. The view is invalidated when they move to the next "string"
 * </ol>
 *
 * Furthermore we:
 *
 * <ol>
 * <li>Offer a FIFO model where the reader (in a separate thread) can chase the writer but there is not an inordinate
 * amount of synchronization overhead (synchronization happens at the block level, not the "string" level).
 * <li>Have the ability to make multiple Readers which pass over the same underlying data. This is our low-drama way of
 * allowing our client to make multiple passes over the data, without complicating the iteration interface, with, e.g.,
 * a reset method.
 * <li>Use a linked-list structure so that when all existing readers have move passed a block of data, that block can be
 * freed by the garbage collector without any explicit action taken by the reader.
 * </ol>
 *
 * If you are familiar with the structure of our inference, you may initially think that this reader-chasing-writer
 * garbage collection trick doesn't buy us much because we have a two-phase parser. However, when the inferencer has
 * gotten to the last parser in its set of allowable parsers (say, the String parser), or the user has specified that
 * there is only one parser for this column, then the code doesn't need to do any inference and can parse the column in
 * one pass. In this case, when the reader stays caught up with the writer, we are basically just buffering one block of
 * data, not the whole file.
 *
 * <p>
 * The implementation used here is to look at the "string" being added to the writer and categorize it along two
 * dimensions:
 *
 * <ul>
 * <li>Small vs large
 * <li>Byte vs char
 * </ul>
 *
 * These dimensions are broken out in the following way:
 * <ul>
 * <li>Small byte "strings" are packed into a byte block, and we maintain a linked list of these byte blocks.
 * <li>"Large" byte "strings" are stored directly, meaning a byte[] array is allocated for their data, then a reference
 * to that array is added to a byte-array block. (And again, we maintain a linked list of these byte-array blocks). It
 * is not typical for CSV data to contain a cell this large, but the feature is there for completeness. We do not want
 * want large "strings" to contaminate our packed byte blocks because they would not likely pack into them tightly (it
 * would become more likely to have allocated blocks with unused storage at the end, because the last big string
 * wouldn't fit in the current block). It's OK to keep them on their own because by definition, large "strings" are not
 * going to have much overhead, as a percentage of the size of their text content.
 * </ul>
 */
public final class DenseStorageWriter {
    /** Constructor */
    public static Pair<DenseStorageWriter, DenseStorageReader> create(final boolean concurrent) {
        final int maxUnobservedBlocks = concurrent ? DenseStorageConstants.MAX_UNOBSERVED_BLOCKS : Integer.MAX_VALUE;
        final Object syncRoot = new Object();
        final Semaphore semaphore = new Semaphore(maxUnobservedBlocks);
        // A placeholder node to hold the "next" field for both writer and reader.
        final QueueNode headNode = new QueueNode(null, 0, 0, null, 0, 0);
        final DenseStorageWriter writer = new DenseStorageWriter(syncRoot, semaphore, headNode);
        final DenseStorageReader reader = new DenseStorageReader(syncRoot, semaphore, headNode);
        return new Pair<>(writer, reader);
    }

    private final Object syncRoot;
    private final Semaphore semaphore;
    private QueueNode tail;

    private byte[] packedBuffer = new byte[DenseStorageConstants.PACKED_QUEUE_SIZE];
    private int packedBegin = 0;
    private int packedCurrent = 0;

    private byte[][] largeArrayBuffer = new byte[DenseStorageConstants.LARGE_ARRAY_QUEUE_SIZE][];
    private int largeArrayBegin = 0;
    private int largeArrayCurrent = 0;

    private final ByteSlice controlWordByteSlice = new ByteSlice(new byte[4], 0, 4);

    public DenseStorageWriter(Object syncRoot, Semaphore semaphore, QueueNode tail) {
        this.syncRoot = syncRoot;
        this.semaphore = semaphore;
        this.tail = tail;
    }

    /**
     * Append a {@link ByteSlice} to the queue. The data will be diverted to one of the two specialized underlying
     * queues, depending on its size.
     */
    public void append(final ByteSlice bs) {
        final int size = bs.size();
        if (size >= DenseStorageConstants.LARGE_THRESHOLD) {
            final byte[] largeArray = new byte[size];
            bs.copyTo(largeArray, 0);
            addControlWord(DenseStorageConstants.LARGE_BYTE_ARRAY_SENTINEL);
            addLargeArray(largeArray);
        } else {
            addControlWord(size);
            addBytes(bs);
        }
    }

    /** Call this method to indicate when you are finished writing to the queue. */
    public void finish() {
        addControlWord(DenseStorageConstants.END_OF_STREAM_SENTINEL);
        flush();
    }

    private void addControlWord(int controlWord) {
        final byte[] data = controlWordByteSlice.data();
        data[0] = (byte)controlWord;
        data[1] = (byte)(controlWord >>> 8);
        data[2] = (byte)(controlWord >>> 16);
        data[3] = (byte)(controlWord >>> 24);
        addBytes(controlWordByteSlice);
    }

    private void addBytes(ByteSlice bs) {
        final int sliceSize = bs.size();
        if (sliceSize == 0) {
            return;
        }
        assert sliceSize <= DenseStorageConstants.PACKED_QUEUE_SIZE;

        if (packedCurrent + sliceSize > packedBuffer.length) {
            flush();
            packedBuffer = new byte[DenseStorageConstants.PACKED_QUEUE_SIZE];
            packedBegin = 0;
            packedCurrent = 0;
        }
        bs.copyTo(packedBuffer, packedCurrent);
        packedCurrent += sliceSize;
    }

    private void addLargeArray(byte[] largeArray) {
        if (largeArrayCurrent == largeArrayBuffer.length) {
            flush();
            largeArrayBuffer = new byte[DenseStorageConstants.LARGE_ARRAY_QUEUE_SIZE][];
            largeArrayBegin = 0;
            largeArrayCurrent = 0;
        }
        largeArrayBuffer[largeArrayCurrent] = largeArray;
        ++largeArrayCurrent;
    }

    private void flush() {
        // This new node now owns the following slices (these are half-open intervals)
        // packedBuffer[packedBegin...packedCurrent)
        // largeArrayBuffer[largeArrayBegin...largeArrayCurrent)
        final QueueNode newNode = new QueueNode(
                packedBuffer, packedBegin, packedCurrent,
                largeArrayBuffer, largeArrayBegin, largeArrayCurrent);

        // DenseStorageWriter now owns suffix of the buffers after what the QueueNode owns.
        // packedBuffer[packedCurrent...end)
        // largeArrayBuffer[largeArrayCurrent...end)
        packedBegin = packedCurrent;
        largeArrayBegin = largeArrayCurrent;

        appendNode(newNode);
    }

    private void appendNode(QueueNode newNode) {
        try {
            semaphore.acquire(1);
        } catch (InterruptedException ie) {
            throw new RuntimeException("Thread interrupted", ie);
        }

        synchronized (syncRoot) {
            if (tail.next != null) {
                throw new RuntimeException("next is already set");
            }
            tail.next = newNode;
            tail = newNode;
            syncRoot.notifyAll();
        }
    }
}
