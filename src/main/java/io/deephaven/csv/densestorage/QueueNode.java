package io.deephaven.csv.densestorage;

/**
 * Linked list node that holds data for a {@link DenseStorageWriter} or {@link DenseStorageReader}. All fields are
 * immutable except the "next" and "appendHasBeenObserved" fields. Synchronization for reading/writing the "next" field
 * is managed by the {@link DenseStorageWriter} and {@link DenseStorageReader}.
 */
public final class QueueNode {
    /**
     * Represents packed bytes from 'packedBuffer' in the half-open interval [packedBegin, packedEnd). It is the contract
     * of the code that bytes in this interval are immutable.
     */
    public final byte[] packedBuffer;
    public final int packedBegin;
    public final int packedEnd;

    /**
     * Represents byte[] references from 'largeArrayBuffer' in the half-open interval [largeArrayBegin, largeArrayEnd). It is the contract
     * of the code that the references in this interval (and the underlying bytes they point to) are immutable.
     */
    public final byte[][] largeArryBuffer;
    public final int largeArrayBegin;
    public final int largeArrayEnd;

    public QueueNode next = null;

    /**
     * Whether at least one reader has observed the {@link QueueNode#next} field transitioning from null to non-null.
     * This is used for flow control, so the writer doesn't get too far ahead of the reader.
     */
    public boolean appendHasBeenObserved = false;

    /**
     * Constructor. Sets this queue node to represent the passed-in slices.
     */
    public QueueNode(byte[] packedBuffer, int packedBegin, int packedEnd, byte[][] largeArryBuffer, int largeArrayBegin,
            int largeArrayEnd) {
        this.packedBuffer = packedBuffer;
        this.packedBegin = packedBegin;
        this.packedEnd = packedEnd;
        this.largeArryBuffer = largeArryBuffer;
        this.largeArrayBegin = largeArrayBegin;
        this.largeArrayEnd = largeArrayEnd;
    }
}
