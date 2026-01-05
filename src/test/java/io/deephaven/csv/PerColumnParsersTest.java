package io.deephaven.csv;

import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.parsers.*;
        import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.testutil.*;
        import io.deephaven.csv.util.CsvReaderException;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.util.*;

public class PerColumnParsersTest {
    @Test
    public void zeroToThreeHearts() throws CsvReaderException {
        final String input = "Col0,Col1,Col2,Col3,Col4,Col5\n" +
                "0, 0, 0, 0, 0, 0\n" +
                "1000, 1000, 1000, 1000, 1000, 1000\n" +
                "5000, 5000, 5000, 5000, 5000, 5000\n";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofRefs("Col0", "0", "1000", "5000"),
                        Column.ofValues("Col1", (short)0, (short)1000, (short)5000),
                        Column.ofValues("Col2", (short)0, (short)1000, (short)5000),
                        Column.ofValues("Col3", (short)0, (short)1000, (short)5000),
                        Column.ofValues("Col4", (int)0, (int)1000, (int)5000),
                        Column.ofValues("Col5", (long)0, (long)1000, (long)5000)
                        );

        CsvTestUtil.invokeTest(csvSpecsWithPerColumnParsers(), input, expected);
    }

    private static CsvSpecs csvSpecsWithPerColumnParsers() {
        List<Parser<?>> build = new ArrayList<>();
        build.add(Parsers.STRING);
        build.add(Parsers.BYTE);
        List<Parser<?>> col0Parsers = new ArrayList<>(build);

        build.add(Parsers.SHORT);
        List<Parser<?>> col1Parsers = new ArrayList<>(build);

        build.add(Parsers.INT);
        List<Parser<?>> col2Parsers = new ArrayList<>(build);

        build.add(Parsers.LONG);
        List<Parser<?>> col3Parsers = new ArrayList<>(build);

        List<Parser<?>> col4Parsers = Arrays.asList(Parsers.INT, Parsers.LONG);

        List<Parser<?>> col5Parsers = Collections.singletonList(Parsers.LONG);

        return CsvTestUtil.defaultCsvBuilder()
                .putParsersForIndex(0, col0Parsers)
                .putParsersForIndex(1, col1Parsers)
                .putParsersForIndex(2, col2Parsers)
                .putParsersForIndex(3, col3Parsers)
                .putParsersForIndex(4, col4Parsers)
                .putParsersForIndex(5, col5Parsers)
                .build();
    }
}
