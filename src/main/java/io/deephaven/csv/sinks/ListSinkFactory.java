package io.deephaven.csv.sinks;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class ListSinkFactory {
    public static SinkFactory createFactory() {
        return SinkFactory.ofSimple(
                ListSink::new,
                ListSink::new,
                ListSink::new,
                ListSink::new,
                ListSink::new,
                ListSink::new,
                ListSink::new,
                ListSink::new,
                ListSink::new,
                ListSink::new,
                ListSink::new
        );
    }
}

class ListSink<TARRAY> implements Sink<TARRAY> {
    protected final List<Object> list;

    protected ListSink(int colNum_ignored) {
        this.list = new ArrayList<>();
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
            Object value = isNull[i] ? null : Array.get(src, i);
            list.set(destBeginAsInt + i, value);
        }
    }

    @Override
    public Object getUnderlying() {
        return list;
    }
}
