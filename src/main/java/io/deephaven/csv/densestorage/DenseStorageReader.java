package io.deephaven.csv.densestorage;

import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.csv.util.MutableInt;

import java.util.concurrent.Semaphore;

/** Companion to the {@link DenseStorageWriter}. See the documentation there for details. */
public final class DenseStorageReader {
    private final Semaphore semaphore;
    private QueueNode tail;

    private int[] controlBuffer = null;
    private int controlCurrent = 0;
    private int controlEnd = 0;

    private byte[] packedBuffer = null;
    private int packedCurrent = 0;
    private int packedEnd = 0;

    private byte[][] largeArrayBuffer = null;
    private int largeArrayCurrent = 0;
    private int largeArrayEnd = 0;

    /** Constructor. */
    public DenseStorageReader(Semaphore semaphore, QueueNode head) {
        this.semaphore = semaphore;
        this.tail = head;
    }

    /**
     * Constructor that copies the state of 'other'.
     * @param other The other object
     */
    private DenseStorageReader(DenseStorageReader other) {
        this.semaphore = other.semaphore;
        this.tail = other.tail;
        this.controlBuffer = other.controlBuffer;
        this.controlCurrent = other.controlCurrent;
        this.controlEnd = other.controlEnd;
        this.packedBuffer = other.packedBuffer;
        this.packedCurrent = other.packedCurrent;
        this.packedEnd = other.packedEnd;
        this.largeArrayBuffer = other.largeArrayBuffer;
        this.largeArrayCurrent = other.largeArrayCurrent;
        this.largeArrayEnd = other.largeArrayEnd;
    }

    public DenseStorageReader copy() {
        return new DenseStorageReader(this);
    }

    /**
     * Tries to get the next slice from the queue. Uses data in the 'control' queue to figure out
     * which data queue ('packed' or 'largeArray') the next slice is coming from.
     * @param bs If the method returns true, the contents of this parameter will be updated.
     * @return true if there is more data, and the ByteSlice has been populated. Otherwise, false.
     */
    public boolean tryGetNextSlice(final ByteSlice bs) throws CsvReaderException {
        final int control = tryGetControlWord();
        if (control == DenseStorageConstants.END_OF_STREAM_SENTINEL) {
            return false;
        }
        if (control == DenseStorageConstants.LARGE_BYTE_ARRAY_SENTINEL) {
            getSliceFromLargeArray(bs);
        } else {
            getSliceFromPackedArray(bs, control);
        }
        return true;
    }

    private int tryGetControlWord() throws CsvReaderException {
        while (controlCurrent == controlEnd) {
            if (!tryRefill()) {
                return DenseStorageConstants.END_OF_STREAM_SENTINEL;
            }
        }
        return controlBuffer[controlCurrent++];
    }

    private void getSliceFromLargeArray(ByteSlice bs) throws CsvReaderException {
        while (largeArrayCurrent == largeArrayEnd) {
            if (!tryRefill()) {
                throw new CsvReaderException("Premature end of large array stream");
            }
        }
        byte[] slice = largeArrayBuffer[largeArrayCurrent++];
        bs.reset(slice, 0, slice.length);
    }

    private void getSliceFromPackedArray(ByteSlice bs, int sizeNeeded) throws CsvReaderException {
        while (packedCurrent == packedEnd) {
            if (!tryRefill()) {
                throw new CsvReaderException("Premature end of packed array stream");
            }
        }
        if (packedCurrent + sizeNeeded > packedEnd) {
            int availableSize = packedEnd - packedCurrent;
            throw new CsvReaderException(
                    String.format(
                            "Assertion failure: got short block: expected at least %d, got %d", sizeNeeded, availableSize));
        }
        final int packedEnd = packedCurrent + sizeNeeded;
        bs.reset(packedBuffer, packedCurrent, packedEnd);
        packedCurrent = packedEnd;
    }

    private boolean tryRefill() throws CsvReaderException {
        if (controlCurrent != controlEnd ||
                packedCurrent != packedEnd ||
                largeArrayCurrent != largeArrayEnd) {
            throw new CsvReaderException("Assertion failure: discarding unread data");
        }

        if (tail == QueueNode.END_OF_STREAM_SENTINEL) {
            return false;
        }

        boolean needsRelease;
        synchronized (this) {
            while (tail.next == null) {
                try {
                    wait();
                } catch (InterruptedException ie) {
                    throw new RuntimeException("Thread interrupted", ie);
                }
            }
            needsRelease = !tail.appendHasBeenObserved;
            tail.appendHasBeenObserved = true;
            tail = tail.next;
        }
        if (needsRelease) {
            semaphore.release();
        }

        controlBuffer = tail.controlBuffer;
        controlCurrent = tail.controlBegin;
        controlEnd = tail.controlEnd;

        packedBuffer = tail.packedBuffer;
        packedCurrent = tail.packedBegin;
        packedEnd = tail.packedEnd;

        largeArrayBuffer = tail.largeArryBuffer;
        largeArrayCurrent = tail.largeArrayBegin;
        largeArrayEnd = tail.largeArrayEnd;;

        return tail != QueueNode.END_OF_STREAM_SENTINEL;
    }
}
