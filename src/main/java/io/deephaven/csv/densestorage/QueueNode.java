package io.deephaven.csv.densestorage;

/**
 * Linked list node that holds data for a {@link DenseStorageWriter} or {@link DenseStorageReader}. All fields are
 * immutable except the "next" and "appendHasBeenObserved" fields. Synchronization for reading/writing the "next" field
 * is managed by the {@link DenseStorageWriter} and {@link DenseStorageReader}.
 */
public final class QueueNode {
    /**
     * We are a reader of 'packedBuffer' in the half-open interval [packedBegin, packedEnd) and the rest of the code
     * promises not to touch those values.
     */
    public final byte[] packedBuffer;
    public final int packedBegin;
    public final int packedEnd;

    /**
     * We are a reader of 'largeArrayBuffer' in the half-open interval [largeArrayBegin, largeArrayEnd) and the rest of
     * the code promises not to change the values in that interval.
     */
    public final byte[][] largeArryBuffer;
    public final int largeArrayBegin;
    public final int largeArrayEnd;

    public QueueNode next = null;

    /**
     * Whether at least one reader has observed the {@link QueueNode#next} field transitioning from null to non-null.
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
