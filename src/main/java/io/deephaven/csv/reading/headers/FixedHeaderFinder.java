package io.deephaven.csv.reading.headers;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.reading.ReaderUtil;
import io.deephaven.csv.reading.cells.CellGrabber;
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
            MutableObject<int[]> columnWidthsResult)
            throws CsvReaderException {
        String[] headersToUse;
        // Get user-specified column widths, if any. If not, this will be an array of length 0.
        // UNITS: UTF8 CHARACTERS
        int[] columnWidthsToUse = specs.fixedColumnWidths().stream().mapToInt(Integer::intValue).toArray();
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
            if (columnWidthsToUse.length == 0) {
                // UNITS: UTF8 CHARACTERS
                columnWidthsToUse = inferColumnWidths(headerRow, paddingByte, specs.useUtf32CountingConvention());
            }

            // DESIRED UNITS: UTF8 CHARACTERS
            headersToUse = extractHeaders(headerRow, columnWidthsToUse, paddingByte, specs.useUtf32CountingConvention());
        } else {
            if (columnWidthsToUse.length == 0) {
                throw new CsvReaderException("Can't proceed because hasHeaderRow is false but fixedColumnWidths is unspecified");
            }
            headersToUse = ReaderUtil.makeSyntheticHeaders(columnWidthsToUse.length);
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
        columnWidthsResult.setValue(columnWidthsToUse);
        return headersToUse;
    }

    // RETURNS UNITS: UTF8 CHARACTERS
    private static int[] inferColumnWidths(ByteSlice row, byte delimiterAsByte, boolean useUtf32CountingConvention) {
        // A column start is a non-delimiter character preceded by a delimiter (or present at the start of line).
        // If the start of the line is a delimiter, that is an error.
        final List<Integer> columnWidths = new ArrayList<>();
        final MutableInt charCountResult = new MutableInt();
        boolean prevCharIsDelimiter = false;
        final byte[] data = row.data();
        int numChars = 0;
        int byteIndex = row.begin();
        while (true) {
            if (byteIndex == row.end()) {
                columnWidths.add(numChars);
                return columnWidths.stream().mapToInt(Integer::intValue).toArray();
            }
            // If this character is not a delimiter, but the previous one was, then this is the start of a new column.
            byte ch = data[byteIndex];
            boolean thisCharIsDelimiter = ch == delimiterAsByte;
            if (byteIndex == row.begin() && thisCharIsDelimiter) {
                throw new IllegalArgumentException(
                        String.format("Header row cannot start with the delimiter character '%c'", (char)delimiterAsByte));
            }
            if (!thisCharIsDelimiter && prevCharIsDelimiter) {
                columnWidths.add(numChars);
                numChars = 0;
            }
            prevCharIsDelimiter = thisCharIsDelimiter;
            byteIndex += ReaderUtil.getUtf8LengthAndCharLength(ch, useUtf32CountingConvention, charCountResult);
            numChars += charCountResult.intValue();
        }
    }

    // UNITS: UTF8 CHARACTERS
    private static String[] extractHeaders(ByteSlice row, int[] columnWidths, byte paddingByte,
                                           boolean utf32CountingMode) {
        final int numCols = columnWidths.length;
        if (numCols == 0) {
            return new String[0];
        }
        final int[] byteWidths = new int[numCols];
        final ByteSlice tempSlice = new ByteSlice();
        final int excessBytes = charWidthsToByteWidths(row, columnWidths, utf32CountingMode, byteWidths);
        // Our policy is that the last column gets any excess bytes that are in the row.
        byteWidths[numCols - 1] += excessBytes;
        final String[] result = new String[numCols];

        int beginByte = row.begin();
        for (int colNum = 0; colNum != numCols; ++colNum) {
            final int proposedEndByte = beginByte + byteWidths[colNum];
            final int actualEndByte = Math.min(proposedEndByte, row.end());
            tempSlice.reset(row.data(), beginByte, actualEndByte);
            tempSlice.trimPadding(paddingByte);
            result[colNum] = tempSlice.toString();
            beginByte = actualEndByte;
        }
        return result;
    }

    private static int charWidthsToByteWidths(ByteSlice row, int[] charWidths, boolean utf32CountingMode,
                                              int[] byteWidths) {
        int numCols = charWidths.length;
        if (byteWidths.length != numCols) {
            throw new IllegalArgumentException(String.format("Expected charWidths.length (%d) == byteWidths.length (%d)",
                    charWidths.length, byteWidths.length));
        }
        final MutableInt charCountResult = new MutableInt();
        final byte[] data = row.data();
        int byteCurrent = row.begin();
        int byteStart = byteCurrent;
        int colIndex = 0;
        int charCount = 0;
        while (true) {
            if (colIndex == numCols) {
                // Excess bytes not claimed by any column
                return row.end() - byteCurrent;
            }
            if (charCount == charWidths[colIndex]) {
                byteWidths[colIndex] = byteCurrent - byteStart;
                byteStart = byteCurrent;
                charCount = 0;
                ++colIndex;
                continue;
            }

            final byte ch = data[byteCurrent];
            byteCurrent += ReaderUtil.getUtf8LengthAndCharLength(ch, utf32CountingMode, charCountResult);
            charCount += charCountResult.intValue();
        }
    }
}
