package io.deephaven.csv.reading;

import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.tokenization.RangeTests;

public class ReaderUtil {
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

    /**
     * Calculate the expected length of a UTF-8 sequence, given its first byte.
     * @param firstByte The first byte of the sequence.
     * @return The length of the sequence, in the range 1..4 inclusive.
     */
    public static int getUtf8Length(byte firstByte) {
        if ((firstByte & 0x80) == 0) {
            // 0xxxxxxx
            // 1-byte UTF-8 character aka ASCII.
            // Last code point U+007F
            return 1;
        }
        if ((firstByte & 0xE0) == 0xC0) {
            // 110xxxxx
            // 2-byte UTF-8 character
            // Last code point U+07FF
            return 2;
        }
        if ((firstByte & 0xF0) == 0xE0) {
            // 1110xxxx
            // 3-byte UTF-8 character
            // Last code point U+FFFF
            return 3;
        }
        if ((firstByte & 0xF8) == 0xF0) {
            // 11110xxx
            // 4-byte UTF-8 character. Note: Java encodes all of these in two "char" variables.
            // Last code point U+10FFFF
            return 4;
        }
        throw new IllegalStateException(String.format("0x%x is not a valid starting byte for a UTF-8 sequence",
                firstByte));
    }
}
