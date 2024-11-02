package io.deephaven.csv.reading;

import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.tokenization.RangeTests;
import io.deephaven.csv.tokenization.Tokenizer;
import io.deephaven.csv.util.MutableInt;

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

    /**
     * Look at the UTF-8 characters pointed to by 'row', at the current byteOffset.
     * Ensure that there are enough bytes remaining in row for the full UTF-8 character. Otherwise, throw an Exception.
     * If utf32CountingMode is true or the ch, set numChar
     * If the character is in the Basic Multilingual Plane, or if utf32CountingMode is true, set
     * numChars to 1. Otherw
     *
     * Return the width of that UTF-8 character.
     * @param row
     * @param byteIndex
     * @param utf32coutingMode
     * @param numChars
     * @return
     */
    public static int advanceOneChar(ByteSlice row, int byteOffset, boolean utf32CountingMode, MutableInt numChars) {
        // zamboni_actually_allow_it_if_configured_will_advance_char_count_by_1_or_2;
        final byte[] data = row.data();
        final byte ch = data[byteIndex];
        final int utf8Length = getUtf8Length(ch);
        numChars.setValue(1);
        if (utf8Length > 3) {
            String badChar = "[unknown]";
            if (byteIndex + utf8Length <= row.end()) {
                badChar = new String(data, byteIndex, utf8Length);
            }
            throw new IllegalStateException(
                    String.format("The input character %s lies outside the Unicode Basic Multilingual Plane and is not supported in fixed column width mode",
                            badChar));
        }

        return byteIndex + utf8Length;
    }

}
