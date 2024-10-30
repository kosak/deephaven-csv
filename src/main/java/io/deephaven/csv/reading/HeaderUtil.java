package io.deephaven.csv.reading;

public class HeaderUtil {
    public static String[] makeSyntheticHeaders(int numHeaders) {
        final String[] result = new String[numHeaders];
        for (int ii = 0; ii < result.length; ++ii) {
            result[ii] = "Column" + (ii + 1);
        }
        return result;
    }
}
