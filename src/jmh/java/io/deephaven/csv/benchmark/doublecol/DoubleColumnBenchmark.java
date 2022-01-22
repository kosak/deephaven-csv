package io.deephaven.csv.benchmark.doublecol;

import ch.randelshofer.fastdoubleparser.FastDoubleParser;
import io.deephaven.csv.benchmark.util.BenchmarkResult;
import org.openjdk.jmh.annotations.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Fork(jvmArgs = {"-Xms4G", "-Xmx4G"})
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@State(Scope.Thread)
public class DoubleColumnBenchmark {
    private static final int ROWS = 1_000_000;
    private static final int COLS = 5;
    private static final int OPERATIONS = ROWS * COLS;

    @State(Scope.Benchmark)
    public static class InputProvider {
        private final String[] headers;
        private final byte[] inputText;
        private final double[][] expected;

        public InputProvider() {
            headers = new String[COLS];
            expected = new double[COLS][];
            for (int col = 0; col < COLS; ++col) {
                headers[col] = "Col" + (col + 1);
                expected[col] = new double[ROWS];
            }
            final Random random = new Random(31337);
            final StringBuilder sb = new StringBuilder();
            sb.append(String.join(",", headers)).append('\n');
            for (int row = 0; row < ROWS; ++row) {
                String colSep = "";
                for (int col = 0; col < COLS; ++col) {
                    final double next = random.nextDouble();
                    expected[col][row] = next;
                    sb.append(colSep);
                    sb.append(next);
                    colSep = ",";
                }
                sb.append('\n');
            }
            inputText = sb.toString().getBytes(StandardCharsets.UTF_8);
        }

        public String[] headers() {
            return headers;
        }

        public double[][] expected() {
            return expected;
        }

        public InputStream makeStream() {
            return new ByteArrayInputStream(inputText);
        }
    }

    /**
     * For the purpose of benchmarking, we reuse the same storage because we're trying to focus on the cost of parsing,
     * not storage. Also we happen to know the table size beforehand, so we preallocate it. In a real application you
     * would use a growable collection type like TIntArrayList instead.
     */
    @State(Scope.Thread)
    public static class ReusableStorage {
        // We happen to know size of the output. But if not, we could have used a growable collection type instead.
        private final double[][] output;

        public ReusableStorage() {
            output = new double[COLS][];
            for (int col = 0; col < COLS; ++col) {
                output[col] = new double[ROWS];
            }
        }

        public double[][] output() {
            return output;
        }
    }

    BenchmarkResult<double[]> result;

    @TearDown(Level.Invocation)
    public void check(final InputProvider input) {
        if (!Arrays.deepEquals(input.expected, result.columns())) {
            throw new RuntimeException("Expected != actual");
        }
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void deephaven(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = DoubleColumnParserDeephaven.read(input.makeStream(), storage.output());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void apache(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = DoubleColumnParserApache.read(input.makeStream(), storage.output(), Double::parseDouble);
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void apacheFastDoubleParser(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = DoubleColumnParserApache.read(input.makeStream(), storage.output(), FastDoubleParser::parseDouble);
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void fastCsv(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = DoubleColumnParserFastCsv.read(input.makeStream(), storage.output(), Double::parseDouble);
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void fastCsvFastDoubleParser(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = DoubleColumnParserFastCsv.read(input.makeStream(), storage.output(), FastDoubleParser::parseDouble);
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void jackson(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = DoubleColumnParserJacksonCsv.read(input.makeStream(), input.headers(), storage.output());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void openCsv(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = DoubleColumnParserOpenCsv.read(input.makeStream(), storage.output(), Double::parseDouble);
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void openCsvFastDoubleParser(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = DoubleColumnParserOpenCsv.read(input.makeStream(), storage.output(), FastDoubleParser::parseDouble);
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void simpleFlatMapper(final InputProvider input, final ReusableStorage storage)
            throws Exception {
        result = DoubleColumnParserSimpleFlatMapper.read(input.makeStream(), storage.output(), Double::parseDouble);
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void simpleFlatMapperFastDoubleParser(final InputProvider input, final ReusableStorage storage)
            throws Exception {
        result = DoubleColumnParserSimpleFlatMapper.read(input.makeStream(), storage.output(),
                FastDoubleParser::parseDouble);
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void superCsv(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = DoubleColumnParserSuperCsv.read(input.makeStream(), storage.output(), Double::parseDouble);
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void superCsvFastDoubleParser(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = DoubleColumnParserSuperCsv.read(input.makeStream(), storage.output(), FastDoubleParser::parseDouble);
    }
}
