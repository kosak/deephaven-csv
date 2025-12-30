package io.deephaven.csv;

import io.deephaven.csv.parsers.Parser;
import io.deephaven.csv.parsers.Parsers;
import io.deephaven.csv.testutil.Column;
import io.deephaven.csv.testutil.ColumnSet;
import io.deephaven.csv.testutil.CsvTestUtil;
import io.deephaven.csv.util.CsvReaderException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class MultipleCustomParsersTest {
    @Test
    public void zeroToThreeHearts() throws CsvReaderException {
        final String input = "Key,Value\n" +
                "A,hello\n" +
                "B,❤hello\n" +
                "C,❤hello❤\n" +
                "C,❤he❤llo❤\n";



        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofRefs("Key", "A", "B", "C", "D"),
                        Column.ofRefs("Value",
                                new ZeroThroughThreeValue("hello"),
                                new ZeroThroughThreeValue("❤hello"),
                                new ZeroThroughThreeValue("❤hello❤"),
                                new ZeroThroughThreeValue("❤he❤llo❤")
                        ));

        CsvTestUtil.invokeTest(csvSpecsWithHearts(), input, expected);
    }

    private static CsvSpecs csvSpecsWithHearts() {
        HeartParser zeroThroughThree = new HeartParser(0, 3, s -> new ZeroThroughThreeValue(s));
        HeartParser twoThroughFour = new HeartParser(2, 4, s -> new TwoThroughFourValue(s));
        HeartParser zeroThroughFive = new HeartParser(0, 5, s -> new ZeroThroughFiveValue(s));

        List<Parser<?>> parsers = new ArrayList<>(Parsers.DEFAULT);
        parsers.add(zeroThroughThree);
        parsers.add(twoThroughFour);
        parsers.add(zeroThroughFive);

        return CsvTestUtil.defaultCsvBuilder().parsers(parsers).build();
    }
}
