package io.deephaven.csv.benchmark.intcol;

import io.deephaven.csv.benchmark.util.BenchmarkResult;
import io.deephaven.csv.benchmark.util.TableMaker;
import io.deephaven.csv.benchmark.util.Util;
import org.openjdk.jmh.annotations.*;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@Fork(value = 2, jvmArgs = {"-Xms32G", "-Xmx32G"})
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@State(Scope.Thread)
public class IntColumnBenchmark {
    public static final int ROWS = 1_000_000;
    public static final int COLS = 5;
    public static final int OPERATIONS = ROWS * COLS;

    @State(Scope.Benchmark)
    public static class InputProvider {
        public final TableMaker<int[]> tableMaker;

        public InputProvider() {
            final Random rng = new Random(31337);
            tableMaker = new TableMaker<>(rng, ROWS, COLS, int[]::new, int[][]::new,
                    (r, col, begin, end) -> {
                        while (begin != end) {
                            col[begin++] = r.nextInt();
                        }
                    },
                    (sb, col, rowIndex) -> sb.append(col[rowIndex]));
        }
    }

    /**
     * For the purpose of benchmarking, we reuse the same storage because we're trying to focus on the cost of parsing,
     * not allocating storage.
     */
    @State(Scope.Thread)
    public static class ReusableStorage {
        // We happen to know the size of the output beforehand. Otherwise we would have used a growable collection.
        public final int[][] output = Util.makeArray(ROWS, COLS, int[]::new, int[][]::new);
    }

    BenchmarkResult<int[]> result;

    @TearDown(Level.Invocation)
    public void check(final InputProvider input) {
        input.tableMaker.check(result.columns());
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void deephaven(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = IntColumnParserDeephaven.read(input.tableMaker.makeStream(), storage.output);
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void apache(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = IntColumnParserApache.read(input.tableMaker.makeStream(), storage.output);
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void fastCsv(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = IntColumnParserFastCsv.read(input.tableMaker.makeStream(), storage.output);
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void jackson(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = IntColumnParserJacksonCsv.read(input.tableMaker.makeStream(), input.tableMaker.headers(),
                storage.output);
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void openCsv(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = IntColumnParserOpenCsv.read(input.tableMaker.makeStream(), storage.output);
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void simpleFlatMapper(final InputProvider input, final ReusableStorage storage)
            throws Exception {
        result = IntColumnParserSimpleFlatMapper.read(input.tableMaker.makeStream(), storage.output);
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void superCsv(final InputProvider input, final ReusableStorage storage) throws Exception {
        result = IntColumnParserSuperCsv.read(input.tableMaker.makeStream(), storage.output);
    }
}
