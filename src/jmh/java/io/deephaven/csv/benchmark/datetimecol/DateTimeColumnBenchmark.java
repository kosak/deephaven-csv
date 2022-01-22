package io.deephaven.csv.benchmark.datetimecol;

import io.deephaven.csv.benchmark.util.BenchmarkResult;
import org.openjdk.jmh.annotations.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Fork(jvmArgs = {"-Xms4G", "-Xmx4G"})
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@State(Scope.Thread)
public class DateTimeColumnBenchmark {
    private static final int ROWS = 1_000_000;
    private static final int COLS = 5;
    private static final int OPERATIONS = ROWS * COLS;

    @State(Scope.Benchmark)
    public static class InputProvider {
        private final String[] headers;
        private final byte[] inputText;
        private final long[][] expected;

        public InputProvider() {
            headers = new String[COLS];
            expected = new long[COLS][];
            for (int col = 0; col < COLS; ++col) {
                headers[col] = "Col" + (col + 1);
                expected[col] = new long[ROWS];
            }

            final Random rng = new Random(31337);
            final StringBuilder sb = new StringBuilder();
            sb.append(String.join(",", headers)).append('\n');

            for (int row = 0; row < ROWS; ++row) {
                String colSep = "";
                for (int col = 0; col < COLS; ++col) {
                    final int yyyy = 2000 + rng.nextInt(1000);
                    final int MM = 1 + rng.nextInt(12);
                    final int dd = 1 + rng.nextInt(28);
                    final int hh = rng.nextInt(24);
                    final int mm = rng.nextInt(60);
                    final int ss = rng.nextInt(60);
                    final int nanos = rng.nextInt(1_000_000_000);
                    final ZonedDateTime zdt = ZonedDateTime.of(yyyy, MM, dd, hh, mm, ss, nanos, ZoneOffset.UTC);
                    final long zdtSeconds = zdt.toEpochSecond();
                    final int zdtNanos = zdt.getNano();
                    expected[col][row] = zdtSeconds * 1_000_000_000 + zdtNanos;
                    sb.append(colSep).append(zdt);
                    colSep = ",";
                }
                sb.append('\n');
            }
            inputText = sb.toString().getBytes(StandardCharsets.UTF_8);
        }

        public String[] headers() {
            return headers;
        }

        public long[][] expected() {
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
        private final long[][] output;

        public ReusableStorage() {
            output = new long[COLS][];
            for (int col = 0; col < COLS; ++col) {
                output[col] = new long[ROWS];
            }
        }

        public long[][] output() {
            return output;
        }
    }

    BenchmarkResult<long[]> result;

    @TearDown(Level.Invocation)
    public void check(final InputProvider input) {
        if (!Arrays.deepEquals(input.expected, result.columns())) {
            throw new RuntimeException("Expected != actual");
        }
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void deephaven(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = DateTimeColumnParserDeephaven.read(input.makeStream(), storage.output());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void apache(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = DateTimeColumnParserApache.read(input.makeStream(), storage.output());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void fastCsv(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = DateTimeColumnParserFastCsv.read(input.makeStream(), storage.output());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void jackson(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = DateTimeColumnParserJacksonCsv.read(input.makeStream(), input.headers(), storage.output());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void openCsv(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = DateTimeColumnParserOpenCsv.read(input.makeStream(), storage.output());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void simpleFlatMapper(final InputProvider input, final ReusableStorage storage)
            throws Exception {
        result = DateTimeColumnParserSimpleFlatMapper.read(input.makeStream(), storage.output());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void superCsv(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = DateTimeColumnParserSuperCsv.read(input.makeStream(), storage.output());
    }
}
