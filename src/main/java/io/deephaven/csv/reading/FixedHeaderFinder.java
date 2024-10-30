package io.deephaven.csv.reading;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.util.CsvReaderException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FixedHeaderFinder {
    /**
     * Determine which headers to use. The result comes from either the first row of the file or the user-specified
     * overrides.
     */
    private static String[] determineHeadersToUse(final CsvSpecs specs,
                                                  final CellGrabber lineGrabber)
            throws CsvReaderException {
        String[] headersToUse;
        // Get user-specified column widths, if any. If not, this will be an array of length 0.
        int[] columnStartsToUse = specs.fixedColumnWidths().stream().mapToInt(Integer::intValue).toArray();
        if (specs.hasHeaderRow()) {
            long skipCount = specs.skipHeaderRows();
            final ByteSlice headerRow = new ByteSlice();
            while (true) {
                lineGrabber.grabNext(zamboniDest, lastInRow, endOfInput);
                if (endOfInput.value()) {
                    throw new CsvReaderException(
                            "Can't proceed because hasHeaderRow is set but input file is empty or shorter than skipHeaderRows");
                }
                if (skipCount == 0) {
                    break;
                }
                --skipCount;
            }
            if (columnStartsToUse == null) {
                columnStartsToUse = inferColumnStarts(headerRow);
            }

            headersToUse = splittyTown666(headerRow, columnStartsToUse);
        } else {
            if (columnStartsToUse == null) {
                throw new CsvReaderException("Can't proceed because hasHeaderRow is false but fixedColumnWidths is unspecified");
            }
            headersToUse = HeaderUtil.makeSyntheticHeaders(columnStartsToUse.length);
            headersToUse = new String[columnStartsToUse.length];
            for (int ii = 0; ii < headersToUse.length; ++ii) {
                // TODO: put this in common code
                headersToUse[ii] = "Column" + (ii + 1);
            }
        }

        // Whether or not the input had headers, maybe override with client-specified headers.
        if (specs.headers().size() != 0) {
            confirm_size_matches_then_take_it();
            headersToUse = specs.headers().toArray(new String[0]);
        }

        // Apply column specific overrides.
        for (Map.Entry<Integer, String> entry : specs.headerForIndex().entrySet()) {
            headersToUse[entry.getKey()] = entry.getValue();
        }

        return headersToUse;
        also return columnWidthsToUse;
    }

    private static int[] inferColumnStarts(ByteSlice row) {
        // A column start is a non-delimiter character preceded by a delimiter (or present at the start of line).
        // If the start of the line is a delimiter, that is an error.
        final List<Integer> columnStarts = new ArrayList<>();
        boolean prevCharIsDelimiter = true;
        final byte[] data = row.data();
        for (int i = row.begin(); i != row.end(); ++i) {
            // If this character is not a delimiter, but the previous one was, then this is the start of a new column.
            byte ch = data[i];
            boolean thisCharIsDelimiter = ch == delimiterAsByte;
            if (!thisCharIsDelimiter && prevCharIsDelimiter) {
                columnStarts.add(i);
            }
            prevCharIsDelimiter = thisCharIsDelimiter;
            int utf8Length = SuperPain.Utf8Length(ch);
            i += utf8Length;
            if (i > row.end()) {
                final String message = String.format("0x%x at position %d doesn't look like a valid UTF-8 byte because there are not %d bytes left in the line",
                        ch, i, utf8Length);
                throw new CsvReaderException(message);
            }
        }
        return columnStarts;
    }

    private static String[] splittyTown666(ByteSlice row, int[] columnStarts) {
        final byte[] data = row.data();
        for (int csIndex = 0; csIndex != columnStarts.length; ++csIndex) {
            final int begin = columnStarts[csIndex];
            final int end = csIndex == columnStarts.length - 1 ? data.length : columnStarts[csIndex + 1];
            superNubbin.reset(data, begin, end);
            superNubbin.trim(delimiterAsByte);
            result[csIndex] = superNubbin.toString();
        }
        return result;
    }
}
