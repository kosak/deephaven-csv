package io.deephaven.csv.densestorage;

import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.util.Pair;

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
 * The point of this object is to store a sequence of (UTF-8 character sequences represented as bytes),
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
 * The implementation used here is to look at the string being added to the writer and determine whether it is
 * "small" or "large".
 * <p>
 * Small byte strings are packed into a byte block which will contain many such small strings, and we maintain a linked
 * list of these byte blocks. Large byte strings are not packed but rather copied to their own byte[] array, then a reference
 * to that array is added to a byte-array block. (And again, we maintain a linked list of these byte-array blocks).
 * We do not want large strings to contaminate our packed byte blocks because they would not likely pack into them tightly.
 *
 * Logically this class manages two queues: the "packed" queue and the "large array" queue.
 * The "packed" queue contains control words and bytes for small strings. The "large array" queue contains byte[]
 * arrays for large strings.
 *
 * These are the various write operations:
 *
 * <ul>
 * <li>Write a small string: The length (4 bytes, nonnegative) is written to the packed queue, and then the UTF-8 bytes of the string.</li>
 * <li>Write a large string: A special sentinel value (4 bytes with value DenseStorageConstants.LARGE_BYTE_ARRAY_SENTINEL) is written to the packed queue,
 * and then the byte[] reference is appended to the large array queue.</li>
 * <li>End of input: A special sentinel value (4 bytes,  with value DenseStorageConstants.END_OF_STREAM_SENTINEL) is written to the packed queue.</li>
 * </ul>
 *
 * One issue is that the storage array underlying the queues may fill at different rates, but when we flush we still need to notify the reader of the current state of both queues,
 * even if the underlying arrays are only partially written. To solve this, we send slices from writer to reader via the QueueNode. Put another way, the flush() operation will create
 * a QueueNode having a slice representing all the packed bytes since the last flush, and a slice representing all the large array references since the last flush. Thus multiple
 * QueueNodes may refer to the same underlying buffers, but they will be different slices of those buffers.
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
        data[0] = (byte) controlWord;
        data[1] = (byte) (controlWord >>> 8);
        data[2] = (byte) (controlWord >>> 16);
        data[3] = (byte) (controlWord >>> 24);
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
