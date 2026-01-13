package io.deephaven.csv.testutil;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.parsers.Parsers;
import io.deephaven.csv.util.CsvReaderException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.stream.Stream;


public class ParserFidelityTest {
    public enum ParseFidelity {
        /** The CSV Parser and the Java Parser return exactly the same value */
        SAME,

        /** The CSV Parser and the Java Parser return different values */
        DIFFERENT,

        /** The CSV Parser throws an exception */
        PARSE_FAIL,
    };

    @ParameterizedTest
    @MethodSource("provideTuplesForTestFidelity")
    public void fidelity(String input, ParseFidelity strictFloatFidelity,
                             ParseFidelity fastFloatFidelity, ParseFidelity doubleFidelity) throws CsvReaderException {

        checkStrictFloat(input, strictFloatFidelity);
        checkFastFloat(input, fastFloatFidelity);
        checkDouble(input, doubleFidelity);
    }

    private static Stream<Arguments> provideTuplesForTestFidelity() {
        return Stream.of(
                Arguments.of("NaN", ParseFidelity.SAME, ParseFidelity.SAME, ParseFidelity.SAME)
        );
    }


//
//
//
//    testFidelity("-Infinity", ParseFidelity.SAME, ParseFidelity.SAME, ParseFidelity.SAME);
//        testFidelity("NaN", ParseFidelity.SAME, ParseFidelity.SAME, ParseFidelity.SAME);
//
//    }

    @Test
    public void testFloatStrict() throws CsvReaderException {
        final String[] inputs = {
                "NaN",
                "-Infinity",
                "+Infinity",
                "0.1",
                "3.4028235E38",  // Float.MaxValue
                "1.4E-45",  // Float.MinValue
                "1.4798235515974676E-17"
        };

        final float[] expectedValues = new float[inputs.length];
        for (int i = 0; i != inputs.length; ++i) {
            expectedValues[i] = Float.parseFloat(inputs[i]);
        }

        final String source = "Values\n" + String.join("\n", inputs);

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues("Values", expectedValues));

        final CsvSpecs specs = CsvTestUtil.defaultCsvBuilder().parsers(Collections.singletonList(
                Parsers.FLOAT_STRICT)).build();
        CsvTestUtil.invokeTest(specs, source, expected);
    }

    @Test
    public void testFloatFast() throws CsvReaderException {
        final String[] inputs = {
                "NaN",
                "-Infinity",
                "+Infinity",
                "0.1",
                "3.4028235E38",  // Float.MaxValue
                "1.4E-45",  // Float.MinValue
                "1.4798235515974676E-17"
        };

        final float[] expectedValues = new float[inputs.length];
        for (int i = 0; i != inputs.length; ++i) {
            expectedValues[i] = Float.parseFloat(inputs[i]);
        }

        final String source = "Values\n" + String.join("\n", inputs);

        final ColumnSet expected =
                ColumnSet.of(
                        Column.ofValues("Values", expectedValues));

        final CsvSpecs specs = CsvTestUtil.defaultCsvBuilder().parsers(Collections.singletonList(
                Parsers.FLOAT_FAST)).build();
        CsvTestUtil.invokeTest(specs, source, expected);
    }
}
