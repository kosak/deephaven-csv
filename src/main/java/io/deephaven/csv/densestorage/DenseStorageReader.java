package io.deephaven.csv.densestorage;

import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.csv.util.MutableInt;

import java.util.concurrent.Semaphore;

/** Companion to the {@link DenseStorageWriter}. See the documentation there for details. */
public final class DenseStorageReader {
    private final Semaphore semaphore;
    private QueueNode tail;

    private int[] controlBuffer = new int[0];
    private int controlCurrent = 0;

    private byte[] packedBuffer = new byte[0];
    private int packedCurrent = 0;

    private byte[][] largeArrayBuffer = new byte[0][];
    private int largeArrayCurrent = 0;

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
        this.packedBuffer = other.packedBuffer;
        this.packedCurrent = other.packedCurrent;
        this.largeArrayBuffer = other.largeArrayBuffer;
        this.largeArrayCurrent = other.largeArrayCurrent;
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
        while (controlCurrent == controlBuffer.length) {
            if (!tryRefill()) {
                return DenseStorageConstants.END_OF_STREAM_SENTINEL;
            }
        }
        return controlBuffer[controlCurrent++];
    }

    private void getSliceFromLargeArray(ByteSlice bs) throws CsvReaderException {
        while (largeArrayCurrent == largeArrayBuffer.length) {
            if (!tryRefill()) {
                throw new CsvReaderException("Premature end of large array stream");
            }
        }
        byte[] slice = largeArrayBuffer[largeArrayCurrent++];
        bs.reset(slice, 0, slice.length);
    }

    private void getSliceFromPackedArray(ByteSlice bs, int sizeNeeded) throws CsvReaderException {
        while (packedCurrent == packedBuffer.length) {
            if (!tryRefill()) {
                throw new CsvReaderException("Premature end of packed array stream");
            }
        }
        if (packedCurrent + sizeNeeded > packedBuffer.length) {
            int availableSize = packedBuffer.length - packedCurrent;
            throw new CsvReaderException(
                    String.format(
                            "Assertion failure: got short block: expected at least %d, got %d", sizeNeeded, availableSize));
        }
        final int packedEnd = packedCurrent + sizeNeeded;
        bs.reset(packedBuffer, packedCurrent, packedEnd);
        packedCurrent = packedEnd;
    }

    private boolean tryRefill() throws CsvReaderException {
        if (controlCurrent != controlBuffer.length ||
                packedCurrent != packedBuffer.length ||
                largeArrayCurrent != largeArrayBuffer.length) {
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

        return tail != QueueNode.END_OF_STREAM_SENTINEL;
    }
}
