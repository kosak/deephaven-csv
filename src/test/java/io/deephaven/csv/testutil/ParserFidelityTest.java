package io.deephaven.csv.testutil;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.parsers.Parser;
import io.deephaven.csv.parsers.Parsers;
import io.deephaven.csv.reading.CsvReader;
import io.deephaven.csv.util.CsvReaderException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.InputStream;
import java.lang.reflect.Array;
import java.util.Arrays;
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

        final Float expectedFloat = Float.parseFloat(input);
        final Double expectedDouble = Double.parseDouble(input);

        checkHelper(input, expectedFloat, Parsers.FLOAT_STRICT, strictFloatFidelity);
        checkHelper(input, expectedFloat, Parsers.FLOAT_FAST, fastFloatFidelity);
        checkHelper(input, expectedDouble, Parsers.DOUBLE, doubleFidelity);
    }

    private static void checkHelper(String input, Object expectedValue, Parser<?> parser,
                                    ParseFidelity fidelity) {
        final String source = "Values\n" + input + "\n";

        final CsvSpecs specs = CsvTestUtil.defaultCsvBuilder().parsers(Collections.singletonList(parser)).build();
        final InputStream stream = CsvTestUtil.toInputStream(source);
        try {
            CsvReader.Result result = CsvReader.read(specs, stream, CsvTestUtil.makeMySinkFactory());
            Assertions.assertThat(result.numCols()).isEqualTo(1);
            Assertions.assertThat(result.numRows()).isEqualTo(1);

            final Object array = result.columns()[0].data();
            final Object element0 = Array.get(array, 0);
            boolean isSame = element0.equals(expectedValue);

            if (isSame) {
                Assertions.assertThat(fidelity).isEqualTo(ParseFidelity.SAME);
            } else {
                Assertions.assertThat(fidelity).isEqualTo(ParseFidelity.DIFFERENT);
            }
        } catch (CsvReaderException e) {
            Assertions.assertThat(fidelity).isEqualTo(ParseFidelity.PARSE_FAIL);
        }
    }
}
