package io.deephaven.csv.benchmark.stringcol;

import io.deephaven.csv.benchmark.util.BenchmarkResult;
import org.openjdk.jmh.annotations.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Fork(jvmArgs = {"-Xms2G", "-Xmx2G"})
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@State(Scope.Thread)
public class StringColumnBenchmark {
    private static final int STRING_LENGTH = 50;
    private static final int ROWS = 1_000_000;
    private static final int COLS = 5;
    private static final int OPERATIONS = ROWS * COLS;

    @State(Scope.Benchmark)
    public static class InputProvider {
        private final String[] headers;
        private final byte[] inputText;
        private final String[][] expected;

        public InputProvider() {
            headers = new String[COLS];
            expected = new String[COLS][];
            for (int col = 0; col < COLS; ++col) {
                headers[col] = "Col" + (col + 1);
                expected[col] = new String[ROWS];
            }
            final Random rng = new Random(31337);
            final StringBuilder sb = new StringBuilder();
            sb.append(String.join(",", headers)).append('\n');
            for (int row = 0; row < ROWS; ++row) {
                String colSep = "";
                for (int col = 0; col < COLS; ++col) {
                    sb.append(colSep);
                    for (int c = 0; c < STRING_LENGTH; ++c) {
                        sb.append((char) ('a' + rng.nextInt(26)));
                    }
                    colSep = ",";
                    expected[col][row] = sb.substring(sb.length() - STRING_LENGTH);
                }
                sb.append('\n');
            }
            inputText = sb.toString().getBytes(StandardCharsets.UTF_8);
        }

        public String[] headers() {
            return headers;
        }

        public String[][] expected() {
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
        private final String[][] output;

        public ReusableStorage() {
            output = new String[COLS][];
            for (int col = 0; col < COLS; ++col) {
                output[col] = new String[ROWS];
            }
        }

        public String[][] output() {
            return output;
        }
    }

    BenchmarkResult<String[]> result;

    @TearDown(Level.Invocation)
    public void check(final InputProvider input) {
        if (!Arrays.deepEquals(input.expected, result.columns())) {
            throw new RuntimeException("Expected != actual");
        }
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void deephaven(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = StringColumnParserDeephaven.read(input.makeStream(), storage.output());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void apache(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = StringColumnParserApache.read(input.makeStream(), storage.output());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void fastCsv(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = StringColumnParserFastCsv.read(input.makeStream(), storage.output());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void jackson(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = StringColumnParserJacksonCsv.read(input.makeStream(), input.headers(), storage.output());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void openCsv(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = StringColumnParserOpenCsv.read(input.makeStream(), storage.output());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void simpleFlatMapper(final InputProvider input, final ReusableStorage storage)
            throws Exception {
        result = StringColumnParserSimpleFlatMapper.read(input.makeStream(), storage.output());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void superCsv(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = StringColumnParserSuperCsv.read(input.makeStream(), storage.output());
    }
}
