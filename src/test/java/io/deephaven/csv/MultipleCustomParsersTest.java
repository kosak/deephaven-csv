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
import java.util.Objects;

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
        HeartParser zeroThroughThreeParser = new HeartParser(0, 3, s -> new ZeroThroughThreeValue(s));
        HeartParser twoThroughFourParser = new HeartParser(2, 4, s -> new TwoThroughFourValue(s));
        HeartParser zeroThroughFivParser = new HeartParser(0, 5, s -> new ZeroThroughFiveValue(s));

        List<Parser<?>> parsers = new ArrayList<>(Parsers.DEFAULT);
        parsers.add(zeroThroughThree);
        parsers.add(twoThroughFour);
        parsers.add(zeroThroughFive);

        return CsvTestUtil.defaultCsvBuilder().parsers(parsers).build();
    }

    private static final class ZeroThroughThreeValue {
        private final String text;

        public ZeroThroughThreeValue(String text) {
            this.text = text;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            ZeroThroughThreeValue that = (ZeroThroughThreeValue) o;
            return Objects.equals(text, that.text);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(text);
        }
    }

    private static final class TwoThroughFourValue {
        private final String text;

        public TwoThroughFourValue(String text) {
            this.text = text;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            TwoThroughFourValue that = (TwoThroughFourValue) o;
            return Objects.equals(text, that.text);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(text);
        }
    }

    private static final class ZeroThroughFiveValue {
        private final String text;

        public ZeroThroughFiveValue(String text) {
            this.text = text;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            ZeroThroughFiveValue that = (ZeroThroughFiveValue) o;
            return Objects.equals(text, that.text);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(text);
        }
    }
}
