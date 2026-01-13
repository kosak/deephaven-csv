package io.deephaven.csv.testutil;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.parsers.Parser;
import io.deephaven.csv.parsers.Parsers;
import io.deephaven.csv.util.CsvReaderException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.stream.Stream;


public class ParserFidelityTest {
    public enum ParseFidelity {
        /**
         * The CSV Parser and the Java Parser return exactly the same value
         */
        SAME,

        /**
         * The CSV Parser and the Java Parser return different values
         */
        DIFFERENT,

        /**
         * The CSV Parser throws an exception
         */
        PARSE_FAIL,
    }

    private static Stream<Arguments> provideTuplesForTestFidelity() {
        return Stream.of(
                Arguments.of("NaN", ParseFidelity.SAME, ParseFidelity.SAME, ParseFidelity.SAME),
                Arguments.of("-Infinity", ParseFidelity.SAME, ParseFidelity.SAME, ParseFidelity.SAME),
                Arguments.of("+Infinity", ParseFidelity.SAME, ParseFidelity.SAME, ParseFidelity.SAME),
                Arguments.of("0.1", ParseFidelity.SAME, ParseFidelity.SAME, ParseFidelity.SAME),
                Arguments.of("3.4028235E38", ParseFidelity.SAME, ParseFidelity.PARSE_FAIL, ParseFidelity.SAME),
                Arguments.of("1.4E-45", ParseFidelity.SAME, ParseFidelity.SAME, ParseFidelity.SAME),
                Arguments.of("1.4798235515974676E-17", ParseFidelity.SAME, ParseFidelity.DIFFERENT, ParseFidelity.SAME)
        );
    }

    @ParameterizedTest
    @MethodSource("provideTuplesForTestFidelity")
    public void fidelity(String input,
                         ParseFidelity strictFloatFidelity,
                         ParseFidelity fastFloatFidelity,
                         ParseFidelity doubleFidelity) {

        Column<float[]> expectedFloat = Column.ofValues("Values", Float.parseFloat(input));
        Column<double[]> expectedDouble = Column.ofValues("Values", Double.parseDouble(input));

        checkHelper(input, expectedFloat, Parsers.FLOAT_STRICT, strictFloatFidelity);
        checkHelper(input, expectedFloat, Parsers.FLOAT_FAST, fastFloatFidelity);
        checkHelper(input, expectedDouble, Parsers.DOUBLE, doubleFidelity);
    }

    private static void checkHelper(String input, Column<?> expectedColumn, Parser<?> parser,
                                    ParseFidelity fidelity) {
        final String source = "Values\n" + input + "\n";
        final ColumnSet expected = ColumnSet.of(expectedColumn);

        final CsvSpecs specs = CsvTestUtil.defaultCsvBuilder().parsers(Collections.singletonList(parser)).build();
        try {
            CsvTestUtil.invokeTest(specs, source, expected);
            Assertions.assertThat(fidelity).isEqualTo(ParseFidelity.SAME);
        } catch (CsvReaderException e) {
            Assertions.assertThat(fidelity).isEqualTo(ParseFidelity.PARSE_FAIL);
        }
    }

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
