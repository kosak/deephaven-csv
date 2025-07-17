package io.deephaven.csv.densestorage;

import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.csv.util.MutableInt;

/** Companion to the {@link DenseStorageWriter}. See the documentation there for details. */
public final class DenseStorageReader {
    private int[] controlBuffer;
    private int controlCurrent;

    private byte[] packedBuffer;
    private int packedCurrent;

    private byte[][] largeArrayBuffer;
    private int largeArrayCurrent;

    /** Constructor. */
    public DenseStorageReader(
            final QueueReader.IntReader controlReader,
            final QueueReader.ByteReader byteReader,
            final QueueReader.ByteArrayReader largeByteArrayReader) {
        this.controlReader = controlReader;
        this.byteReader = byteReader;
        this.largeByteArrayReader = largeByteArrayReader;
        this.intHolder = new MutableInt();
    }

    public DenseStorageReader copy() {
        return new DenseStorageReader(controlReader.copy(), byteReader.copy(), largeByteArrayReader.copy());
    }

    /**
     * Tries to get the next slice from one of the inner QueueReaders. Uses data in the 'controlReader' to figure out
     * which QueueReader the next slice is coming from.
     *
     * @param bs If the method returns true, the contents of this parameter will be updated.
     * @return true if there is more data, and the ByteSlice has been populated. Otherwise, false.
     */
    public boolean tryGetNextSlice(final ByteSlice bs) throws CsvReaderException {
        if (!tryGetControlWord(intHolder)) {
            return false;
        }
        final int control = intHolder.intValue();
        if (control == DenseStorageConstants.LARGE_BYTE_ARRAY_SENTINEL) {
            getSliceFromLargeArray(bs);
        } else {
            getSliceFromPackedArray(bs, control);
        }
        return true;
    }

    private int tryGetControlWord() {
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
}
