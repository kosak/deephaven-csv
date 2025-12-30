package io.deephaven.csv;

import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.parsers.*;
import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.testutil.*;
import io.deephaven.csv.util.CsvReaderException;
import org.jetbrains.annotations.NotNull;
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
                "D,❤he❤llo❤\n";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofRefs("Key", "A", "B", "C", "D"),
                        Column.ofRefs("Value",
                                new TaggedHeartValue(HeartCategory.ZERO_THROUGH_THREE, "hello"),
                                new TaggedHeartValue(HeartCategory.ZERO_THROUGH_THREE, "❤hello"),
                                new TaggedHeartValue(HeartCategory.ZERO_THROUGH_THREE, "❤hello❤"),
                                new TaggedHeartValue(HeartCategory.ZERO_THROUGH_THREE, "❤he❤llo❤")
                        ));

        final MakeCustomColumn makeCustomColumn = (name, obj, size) -> {
            final TaggedHeartValue[] arr = ((List<TaggedHeartValue>) obj).toArray(new TaggedHeartValue[0]);
            return Column.ofArray(name, arr, size);
        };

        CsvTestUtil.invokeTest(csvSpecsWithHearts(), input, expected, CsvTestUtil.makeMySinkFactory(),
                makeCustomColumn);
    }

    @Test
    public void zeroToFiveHearts() throws CsvReaderException {
        final String input = "Key,Value\n" +
                "A,hello\n" +
                "B,❤hello\n" +
                "C,❤❤he❤llo❤❤\n" +
                "D,❤hello❤\n" +
                "E,❤he❤llo❤\n";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofRefs("Key", "A", "B", "C", "D", "E"),
                        Column.ofRefs("Value",
                                new TaggedHeartValue(HeartCategory.ZERO_THROUGH_FIVE, "hello"),
                                new TaggedHeartValue(HeartCategory.ZERO_THROUGH_FIVE, "❤hello"),
                                new TaggedHeartValue(HeartCategory.ZERO_THROUGH_FIVE, "❤❤he❤llo❤❤"),
                                new TaggedHeartValue(HeartCategory.ZERO_THROUGH_FIVE, "❤hello❤"),
                                new TaggedHeartValue(HeartCategory.ZERO_THROUGH_FIVE, "❤he❤llo❤")
                        ));

        final MakeCustomColumn makeCustomColumn = (name, obj, size) -> {
            final TaggedHeartValue[] arr = ((List<TaggedHeartValue>) obj).toArray(new TaggedHeartValue[0]);
            return Column.ofArray(name, arr, size);
        };

        CsvTestUtil.invokeTest(csvSpecsWithHearts(), input, expected, CsvTestUtil.makeMySinkFactory(),
                makeCustomColumn);
    }

    @Test
    public void twoToFourHearts() throws CsvReaderException {
        final String input = "Key,Value\n" +
                "A,❤hello❤\n" +
                "B,❤❤hello❤❤\n" +
                "C,❤hello❤\n" +
                "D,❤he❤llo❤\n";

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofRefs("Key", "A", "B", "C", "D"),
                        Column.ofRefs("Value",
                                new TaggedHeartValue(HeartCategory.TWO_THROUGH_FOUR, "❤hello❤"),
                                new TaggedHeartValue(HeartCategory.TWO_THROUGH_FOUR, "❤❤hello❤❤"),
                                new TaggedHeartValue(HeartCategory.TWO_THROUGH_FOUR, "❤hello❤"),
                                new TaggedHeartValue(HeartCategory.TWO_THROUGH_FOUR, "❤he❤llo❤")
                        ));

        final MakeCustomColumn makeCustomColumn = (name, obj, size) -> {
            final TaggedHeartValue[] arr = ((List<TaggedHeartValue>) obj).toArray(new TaggedHeartValue[0]);
            return Column.ofArray(name, arr, size);
        };

        CsvTestUtil.invokeTest(csvSpecsWithHearts(), input, expected, CsvTestUtil.makeMySinkFactory(),
                makeCustomColumn);
    }

    private static CsvSpecs csvSpecsWithHearts() {
        HeartParser zeroThroughThreeParser = new HeartParser(0, 3, HeartCategory.ZERO_THROUGH_THREE);
        HeartParser twoThroughFourParser = new HeartParser(2, 4, HeartCategory.TWO_THROUGH_FOUR);
        HeartParser zeroThroughFiveParser = new HeartParser(0, 5, HeartCategory.ZERO_THROUGH_FIVE);

        List<Parser<?>> parsers = new ArrayList<>(Parsers.DEFAULT);
        parsers.clear();
        parsers.add(zeroThroughThreeParser);
        parsers.add(twoThroughFourParser);
        parsers.add(zeroThroughFiveParser);

        return CsvTestUtil.defaultCsvBuilder().parsers(parsers).putParserForIndex(0, Parsers.STRING).build();
    }

    private static final class HeartParser implements Parser<TaggedHeartValue[]> {
        private final int minHearts;
        private final int maxHearts;
        private final HeartCategory heartCategory;

        public HeartParser(int minHearts, int maxHearts, HeartCategory heartCategory) {
            this.minHearts = minHearts;
            this.maxHearts = maxHearts;
            this.heartCategory = heartCategory;
        }

        @NotNull
        @Override
        public ParserContext<TaggedHeartValue[]> makeParserContext(GlobalContext gctx, int chunkSize) {
            final Sink<TaggedHeartValue[]> sink = new MyReferenceTypeSink<>();
            return new ParserContext<>(sink, null, DataType.CUSTOM, new TaggedHeartValue[chunkSize]);
        }

        @Override
        public long tryParse(
                GlobalContext gctx,
                ParserContext<TaggedHeartValue[]> pctx,
                IteratorHolder ih,
                long begin,
                long end,
                boolean appending)
                throws CsvReaderException {
            final boolean[] nulls = gctx.nullChunk();

            final Sink<TaggedHeartValue[]> sink = pctx.sink();
            final TaggedHeartValue[] values = pctx.valueChunk();

            long current = begin;
            int chunkIndex = 0;
            do {
                if (chunkIndex == values.length) {
                    sink.write(values, nulls, current, current + chunkIndex, appending);
                    current += chunkIndex;
                    chunkIndex = 0;
                }
                if (current + chunkIndex == end) {
                    break;
                }
                if (gctx.isNullCell(ih)) {
                    nulls[chunkIndex++] = true;
                    continue;
                }
                final ByteSlice bs = ih.bs();

                final int numHearts = countUtf8Hearts(bs);
                if (numHearts < minHearts || numHearts > maxHearts) {
                    break;
                }

                values[chunkIndex] = new TaggedHeartValue(heartCategory, bs.toString());
                nulls[chunkIndex++] = false;
            } while (ih.tryMoveNext());
            sink.write(values, nulls, current, current + chunkIndex, appending);
            return current + chunkIndex;
        }

        // Simplistic code to go looking for the UTF-8 encoding of '❤'.
        // Note that this sliding window search approach is OK because UTF-8 is
        // self-synchronizing. That is, 0xe2 never appear in the middle of a UTF-8 sequence.
        // If it appears, it will be at the start of a UTF-8 sequence.
        private static int countUtf8Hearts(ByteSlice bs) {
            // The UTF-8 encoding of '❤' (U+2764) (HEAVY BLACK HEART)
            final byte utf8Heart0 = (byte)0xe2;
            final byte utf8Heart1 = (byte)0x9d;
            final byte utf8Heart2 = (byte)0xa4;

            if (bs.size() < 3) {
                return 0;
            }

            int result = 0;
            final byte[] data = bs.data();
            for (int i = bs.begin(); i <= bs.end() - 3; ++i) {
                if (data[i] == utf8Heart0 && data[i + 1] == utf8Heart1 && data[i + 2] == utf8Heart2) {
                    ++result;
                }
            }
            return result;
        }
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

        @Override
        public String toString() {
            return heartCategory + ": " + text;
        }
    }
}
