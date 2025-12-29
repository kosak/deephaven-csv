package io.deephaven.csv;

import io.deephaven.csv.parsers.Parsers;
import io.deephaven.csv.testutil.Column;
import io.deephaven.csv.testutil.ColumnSet;
import io.deephaven.csv.testutil.CsvTestUtil;
import io.deephaven.csv.util.CsvReaderException;
import org.junit.jupiter.api.Test;

public class MultipleCustomParsersTest {
    @Test
    public void bug70() throws CsvReaderException {
        final String input = "Key,0to3Hearts,2To4Hearts,5Hearts\n" +
                "A,hello\n" +
                "B,❤hello\n" +
                "C,❤hello❤\n" +
                "C,❤he❤art3❤\n";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofRefs("Coin", "USDT", "USDT", "USDT", "USDT"),
                        Column.ofValues("Change", -49.00787612, -152.686844, -59.92650232, -102.3862566),
                        Column.ofRefs("Remark", null, "穿仓保证金补偿", null, null));
        CsvTestUtil.invokeTest(CsvTestUtil.defaultCsvBuilder().parsers(Parsers.DEFAULT).build(), input, expected);
    }
}
