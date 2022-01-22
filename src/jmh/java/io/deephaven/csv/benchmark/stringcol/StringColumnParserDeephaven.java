package io.deephaven.csv.benchmark.stringcol;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.benchmark.util.ArrayBacked;
import io.deephaven.csv.benchmark.util.BenchmarkResult;
import io.deephaven.csv.benchmark.util.SinkFactories;
import io.deephaven.csv.parsers.Parsers;
import io.deephaven.csv.reading.CsvReader;
import io.deephaven.csv.sinks.SinkFactory;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

public final class StringColumnParserDeephaven {
    public static BenchmarkResult<String[]> read(final InputStream in, final String[][] storage) throws Exception {
        final SinkFactory sinkFactory = SinkFactories.makeSingleUseStringSinkFactory(storage);
        final CsvSpecs specs = CsvSpecs.builder()
                .parsers(List.of(Parsers.STRING))
                .hasHeaderRow(true)
                .build();
        final CsvReader.Result result = CsvReader.read(specs, in, sinkFactory);
        final String[][] data = Arrays.stream(result.columns())
                .map(col -> ((ArrayBacked<String[]>) col).getUnderlyingArray()).toArray(String[][]::new);
        return BenchmarkResult.of(result.numRows(), data);
    }
}
