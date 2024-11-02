package io.deephaven.csv.reading.headers;

import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.tokenization.RangeTests;

public class HeaderUtil {
    public static String[] makeSyntheticHeaders(int numHeaders) {
        final String[] result = new String[numHeaders];
        for (int ii = 0; ii < result.length; ++ii) {
            result[ii] = "Column" + (ii + 1);
        }
        return result;
    }

    /**
     * Trim whitespace from the front and back of the slice.
     *
     * @param cs The slice, modified in-place to have whitespace (if any) removed.
     */
    public static void trimWhitespace(final ByteSlice cs) {
        final byte[] data = cs.data();
        int begin = cs.begin();
        int end = cs.end();
        while (begin != end && RangeTests.isSpaceOrTab(data[begin])) {
            ++begin;
        }
        while (begin != end && RangeTests.isSpaceOrTab(data[end - 1])) {
            --end;
        }
        cs.reset(data, begin, end);
    }
}
