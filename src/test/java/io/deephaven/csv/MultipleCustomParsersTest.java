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
                                TaggedHeartValue.of("hello", HeartCategory.ZERO_THROUGH_THREE),
                                TaggedHeartValue.of("❤hello", HeartCategory.ZERO_THROUGH_THREE),
                                TaggedHeartValue.of("❤hello❤", HeartCategory.ZERO_THROUGH_THREE),
                                TaggedHeartValue.of("❤he❤llo❤", HeartCategory.ZERO_THROUGH_THREE)
                        ));

        CsvTestUtil.invokeTest(csvSpecsWithHearts(), input, expected);
    }

    private static CsvSpecs csvSpecsWithHearts() {
        HeartParser zeroThroughThreeParser = new HeartParser(0, 3, HeartCategory.ZERO_THROUGH_THREE);
        HeartParser twoThroughFourParser = new HeartParser(2, 4, HeartCategory.TWO_THROUGH_FOUR);
        HeartParser zeroThroughFiveParser = new HeartParser(0, 5, HeartCategory.ZERO_THROUGH_FIVE);

        List<Parser<?>> parsers = new ArrayList<>(Parsers.DEFAULT);
        parsers.add(zeroThroughThreeParser);
        parsers.add(twoThroughFourParser);
        parsers.add(zeroThroughFiveParser);

        return CsvTestUtil.defaultCsvBuilder().parsers(parsers).build();
    }

    private static final class HeartParser implements Parser<TaggedHeartValue> {

    }

    private enum HeartCategory { ZERO_THROUGH_THREE, TWO_THROUGH_FOUR, ZERO_THROUGH_FIVE };

    private static final class TaggedHeartValue {
        private final HeartCategory heartCategory;
        private final String text;

        public TaggedHeartValue(HeartCategory heartCategory, String text) {
            this.heartCategory = heartCategory;
            this.text = text;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            TaggedHeartValue that = (TaggedHeartValue) o;
            return heartCategory == that.heartCategory && Objects.equals(text, that.text);
        }

        @Override
        public int hashCode() {
            return Objects.hash(heartCategory, text);
        }
    }
}
