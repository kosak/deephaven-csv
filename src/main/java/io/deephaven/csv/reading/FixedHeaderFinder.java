package io.deephaven.csv.reading;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.tokenization.Tokenizer;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.csv.util.MutableBoolean;
import io.deephaven.csv.util.MutableInt;
import io.deephaven.csv.util.MutableObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FixedHeaderFinder {
    /**
     * Determine which headers to use. The result comes from either the first row of the file or the user-specified
     * overrides.
     */
    public static String[] determineHeadersToUse(
            final CsvSpecs specs,
            final CellGrabber lineGrabber,
            MutableObject<int[]> columnStartsResult)
            throws CsvReaderException {
        String[] headersToUse;
        // Get user-specified column widths, if any. If not, this will be an array of length 0.
        // UNITS: UTF8 CHARACTERS
        int[] columnStartsToUse = specs.fixedColumnWidths().stream().mapToInt(Integer::intValue).toArray();
        if (specs.hasHeaderRow()) {
            long skipCount = specs.skipHeaderRows();
            final ByteSlice headerRow = new ByteSlice();
            MutableBoolean lastInRow = new MutableBoolean();
            MutableBoolean endOfInput = new MutableBoolean();
            while (true) {
                lineGrabber.grabNext(headerRow, lastInRow, endOfInput);
                if (endOfInput.booleanValue()) {
                    throw new CsvReaderException(
                            "Can't proceed because hasHeaderRow is set but input file is empty or shorter than skipHeaderRows");
                }
                if (skipCount == 0) {
                    break;
                }
                --skipCount;
            }
            final byte paddingByte = (byte)specs.delimiter();
            if (columnStartsToUse.length == 0) {
                // UNITS: UTF8 CHARACTERS
                columnStartsToUse = inferColumnStarts(headerRow, paddingByte);
            }

            // DESIRED UNITS: UTF8 CHARACTERS
            headersToUse = extractHeaders(headerRow, columnStartsToUse, paddingByte);
        } else {
            if (columnStartsToUse == null) {
                throw new CsvReaderException("Can't proceed because hasHeaderRow is false but fixedColumnWidths is unspecified");
            }
            headersToUse = HeaderUtil.makeSyntheticHeaders(columnStartsToUse.length);
        }

        // Whether or not the input had headers, maybe override with client-specified headers.
        if (specs.headers().size() != 0) {
            if (specs.headers().size() != headersToUse.length) {
                final String message = String.format("Library determined %d headers; caller overrode with %d headers",
                        headersToUse.length, specs.headers().size());
                throw new CsvReaderException(message);
            }
            headersToUse = specs.headers().toArray(new String[0]);
        }

        // Apply column specific overrides.
        for (Map.Entry<Integer, String> entry : specs.headerForIndex().entrySet()) {
            headersToUse[entry.getKey()] = entry.getValue();
        }

        // DESIRED UNITS: UTF8 CHARACTERS
        columnStartsResult.setValue(columnStartsToUse);
        return headersToUse;
    }

    // RETURNS UNITS: UTF8 CHARACTERS
    private static int[] inferColumnStarts(ByteSlice row, byte delimiterAsByte) {
        // A column start is a non-delimiter character preceded by a delimiter (or present at the start of line).
        // If the start of the line is a delimiter, that is an error.
        final List<Integer> columnStarts = new ArrayList<>();
        boolean prevCharIsDelimiter = true;
        final byte[] data = row.data();
        int charIndex = 0;
        for (int i = row.begin(); i != row.end(); ++i) {
            // If this character is not a delimiter, but the previous one was, then this is the start of a new column.
            byte ch = data[i];
            boolean thisCharIsDelimiter = ch == delimiterAsByte;
            if (!thisCharIsDelimiter && prevCharIsDelimiter) {
                columnStarts.add(charIndex);
            }
            prevCharIsDelimiter = thisCharIsDelimiter;
            final int utf8Length = Tokenizer.getUtf8Length(ch);
            if (utf8Length > 3) {
                String badChar = "[unknown]";
                if (i + utf8Length <= row.end()) {
                    badChar = new String(data, i, utf8Length);
                }
                throw new IllegalStateException(
                        String.format("The input character %s lies outside the Unicode Basic Multilingual Plane and is not supported in fixed column width mode",
                        badChar));
            }
            ++charIndex;
            i += utf8Length;
            if (i > row.end()) {
                throw new IllegalStateException(String.format(
                        "0x%x at position %d doesn't look like a valid UTF-8 sequence because there are not %d bytes left in the line",
                        ch, i, utf8Length));
            }
        }
        return columnStarts.stream().mapToInt(Integer::intValue).toArray();
    }

    // UNITS: UTF8 CHARACTERS
    private static String[] extractHeaders(ByteSlice row, int[] columnWidths) {
        final int numCols = columnWidths.length;
        if (numCols == 0) {
            return new String[0];
        }
        final int[] byteWidths = new int[numCols];
        final MutableInt excessBytes = new MutableInt();
        charWidthsToByteWidths(row, columnWidths, byteWidths, excessBytes);
        // Our policy is that the last column gets any excess bytes that are in the row.
        byteWidths[numCols - 1] += excessBytes.intValue();
        final String[] result = new String[numCols];

        int beginByte = row.begin();
        for (int colNum = 0; colNum != numCols; ++colNum) {
            final int proposedEndByte = beginByte + byteWidths[colNum];
            final int actualEndByte = Math.min(proposedEndByte, row.end());
            result[colNum] = new String(row.data(), beginByte, actualEndByte - beginByte);
        }
        return result;
    }

    private static void charWidthsToByteWidths(ByteSlice row, int[] charWidths, int[] byteWidths,
                                               MutableInt excessBytes) {
        int numCols = charWidths.length;
        if (byteWidths.length != numCols) {
            throw new IllegalArgumentException(String.format("Expected charWidths.length (%d) == byteWidths.length (%d)",
                    charWidths.length, byteWidths.length));
        }
        int byteIndex = row.begin();
        int byteStart = byteIndex;
        int colIndex = 0;
        int charCount = 0;
        while (true) {
            if (colIndex == numCols) {
                excessBytes.setValue(byteIndex - byteStart);
                return;
            }
            if (charCount == charWidths[colIndex]) {
                byteWidths[colIndex] = byteIndex - byteStart;
                byteStart = byteIndex;
                charCount = 0;
                ++colIndex;
                continue;
            }

            byteIndex = advanceByteIndex(row, byteIndex);
            ++charCount;
        }
    }

    private static int advanceByteIndex(ByteSlice row, int byteIndex) {
        final byte[] data = row.data();
        final byte ch = data[byteIndex];
        final int utf8Length = Tokenizer.getUtf8Length(ch);
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
