package io.deephaven.csv.sinks;

import java.lang.reflect.Array;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.UnaryOperator;

public class ListSinkFactory {
    public static final SinkFactory INSTANCE = SinkFactory.ofSimple(
                colNum -> new ListSink<>(colNum, UnaryOperator.identity()),
                colNum -> new ListSink<>(colNum, UnaryOperator.identity()),
                colNum -> new ListSink<>(colNum, UnaryOperator.identity()),
                colNum -> new ListSink<>(colNum, UnaryOperator.identity()),
                colNum -> new ListSink<>(colNum, UnaryOperator.identity()),
                colNum -> new ListSink<>(colNum, UnaryOperator.identity()),
                colNum -> new ListSink<>(colNum, ListSinkFactory::convertToBoolean),
                colNum -> new ListSink<>(colNum, UnaryOperator.identity()),
                colNum -> new ListSink<>(colNum, UnaryOperator.identity()),
                colNum -> new ListSink<>(colNum, ListSinkFactory::convertToInstant),
                colNum -> new ListSink<>(colNum, ListSinkFactory::convertToInstant)
        );

    private static Boolean convertToBoolean(Byte o) {
        return o != 0;
    }

    private static Instant convertToInstant(Long totalNanos) {
        if (totalNanos == null) {
            System.out.println("HOW");
        }
        long seconds = totalNanos / 1_000_000_000;
        long nanos = totalNanos % 1_000_000_000;
        return Instant.ofEpochSecond(seconds, nanos);
    }
}

class ListSink<TARRAY, T, TARGET> implements Sink<TARRAY> {
    private final List<TARGET> list;
    private final Function<T, TARGET> converter;

    public ListSink(int colNum, Function<T, TARGET> converter) {
        // colNum is unused in this sink
        this.list = new ArrayList<>();
        this.converter = converter;
    }

    @Override
    public final void write(
            final TARRAY src,
            final boolean[] isNull,
            final long destBegin,
            final long destEnd,
            boolean appending) {
        if (destBegin == destEnd) {
            return;
        }
        final int destBeginAsInt = Math.toIntExact(destBegin);
        final int destEndAsInt = Math.toIntExact(destEnd);
        final int destSize = Math.toIntExact(destEnd - destBegin);

        // Ensure capacity
        while (list.size() < destEndAsInt) {
            list.add(null);
        }

        // Populate elements
        for (int i = 0; i < destSize; ++i) {
            TARGET converted = null;
            if (!isNull[i]) {
                T element = (T)Array.get(src, i);
                converted = converter.apply(element);
            }
            list.set(destBeginAsInt + i, converted);
        }
    }

    @Override
    public List<TARGET> getUnderlying() {
        return list;
    }
}
