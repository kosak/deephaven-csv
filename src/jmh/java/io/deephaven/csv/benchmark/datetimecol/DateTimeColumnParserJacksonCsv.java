package io.deephaven.csv.benchmark.datetimecol;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import io.deephaven.csv.benchmark.doublecol.DoubleColumnParserJacksonCsv;
import io.deephaven.csv.benchmark.intcol.IntColumnParserJacksonCsv;
import io.deephaven.csv.benchmark.util.BenchmarkResult;
import io.deephaven.csv.benchmark.util.DateTimeParser;

import java.io.InputStream;

public final class DateTimeColumnParserJacksonCsv {
    public static class Row {
        public Row(
                @JsonProperty("Col1") String col1Text,
                @JsonProperty("Col2") String col2Text,
                @JsonProperty("Col3") String col3Text,
                @JsonProperty("Col4") String col4Text,
                @JsonProperty("Col5") String col5Text) {
            col1AsLong = DateTimeParser.parseDateTime(col1Text);
            col2AsLong = DateTimeParser.parseDateTime(col2Text);
            col3AsLong = DateTimeParser.parseDateTime(col3Text);
            col4AsLong = DateTimeParser.parseDateTime(col4Text);
            col5AsLong = DateTimeParser.parseDateTime(col5Text);
        }

        public final long col1AsLong;
        public final long col2AsLong;
        public final long col3AsLong;
        public final long col4AsLong;
        public final long col5AsLong;
    }

    public static BenchmarkResult<long[]> read(final InputStream in, final String[] headers, final long[][] storage) throws Exception {
        if (headers.length != 5) {
            throw new RuntimeException("JacksonCsv benchmark has been special-cased to assume 5 columns");
        }
        final CsvSchema.Builder builder = CsvSchema.builder();
        for (String header : headers) {
            builder.addColumn(header);
        }
        final CsvSchema schema = builder.build().withSkipFirstDataRow(true);
        final MappingIterator<Row> it = new CsvMapper()
                .readerFor(Row.class)
                .with(schema)
                .readValues(in);
        int rowNum = 0;
        while (it.hasNext()) {
            final Row row = it.next();
            storage[0][rowNum] = row.col1AsLong;
            storage[1][rowNum] = row.col2AsLong;
            storage[2][rowNum] = row.col3AsLong;
            storage[3][rowNum] = row.col4AsLong;
            storage[4][rowNum] = row.col5AsLong;
            ++rowNum;
        }
        return BenchmarkResult.of(rowNum, storage);
    }
}
