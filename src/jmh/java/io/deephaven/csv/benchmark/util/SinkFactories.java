package io.deephaven.csv.benchmark.util;

import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.sinks.Source;
import io.deephaven.csv.sinks.SinkFactory;

import java.util.function.Supplier;

public final class SinkFactories {
    public static SinkFactory makeSingleUseIntSinkFactory(final int[][] storage) {
        return SinkFactory.of(null, null,
                null, null,
                SinkSupplier.of(storage), null,
                null, null,
                null, null,
                null, null,
                null,
                null, null,
                null, null,
                null, null,
                null, null);
    }

    public static SinkFactory makeSingleUseDoubleSinkFactory(final double[][] storage) {
        return SinkFactory.of(null, null,
                null, null,
                null, null,
                null, null,
                null, null,
                SinkSupplier.of(storage), null,
                null,
                null, null,
                null, null,
                null, null,
                null, null);
    }

    public static SinkFactory makeSingleUseStringSinkFactory(final String[][] storage) {
        return SinkFactory.of(null, null,
                null, null,
                null, null,
                null, null,
                null, null,
                null, null,
                null,
                null, null,
                SinkSupplier.of(storage), null,
                null, null,
                null, null);
    }

    public static SinkFactory makeSingleUseDateTimeAsLongSinkFactory(final long[][] storage) {
        return SinkFactory.of(null, null,
                null, null,
                null, null,
                null, null,
                null, null,
                null, null,
                null,
                null, null,
                null, null,
                SinkSupplier.of(storage), null,
                null, null);
    }

    private interface SourceSink<TARRAY> extends Source<TARRAY>, Sink<TARRAY> {
    }

    private static final class SinkSupplier<TARRAY> implements Supplier<SourceSink<TARRAY>> {
        public static <TARRAY> SinkSupplier<TARRAY> of(final TARRAY[] items) {
            return new SinkSupplier<>(items);
        }

        private final TARRAY[] items;
        private int nextIndex;

        public SinkSupplier(TARRAY[] items) {
            this.items = items;
            nextIndex = 0;
        }

        @Override
        public SourceSink<TARRAY> get() {
            if (nextIndex == items.length) {
                throw new RuntimeException("Ran out of items to supply.");
            }
            return ArrayBackedSourceSink.of(items[nextIndex++]);
        }
    }

    private static final class ArrayBackedSourceSink<TARRAY> implements SourceSink<TARRAY>, ArrayBacked<TARRAY> {
        public static <TARRAY> ArrayBackedSourceSink<TARRAY> of(final TARRAY storage) {
            return new ArrayBackedSourceSink<>(storage);
        }

        private TARRAY storage;

        public ArrayBackedSourceSink(final TARRAY storage) {
            this.storage = storage;
        }

        @Override
        public void write(final TARRAY src, final boolean[] isNull_unused, final long destBegin, final long destEnd,
                final boolean appending_unused) {
            final int length = Math.toIntExact(destEnd) - Math.toIntExact(destBegin);
            System.arraycopy(src, 0, storage, Math.toIntExact(destBegin), length);
        }

        @Override
        public void read(final TARRAY dest, final boolean[] isNull_unused, final long srcBegin, final long srcEnd) {
            final int length = Math.toIntExact(srcEnd) - Math.toIntExact(srcBegin);
            System.arraycopy(storage, Math.toIntExact(srcBegin), dest, 0, length);
        }

        @Override
        public TARRAY getUnderlyingArray() {
            return storage;
        }
    }
}
