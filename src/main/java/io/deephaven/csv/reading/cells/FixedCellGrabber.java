package io.deephaven.csv.reading.cells;

import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.reading.ReaderUtil;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.csv.util.MutableBoolean;

import java.io.InputStream;

/**
 * This class uses an underlying DelimitedCellGrabber to grab whole lines at a time from the input stream,
 * and then it breaks them into fixed-sized cells to return to the caller.
 */
public class FixedCellGrabber implements CellGrabber {
    /**
     * Makes a degenerate CellGrabber that has no delimiters or quotes and therefore returns whole lines.
     * This is a somewhat quick-and-dirty way to reuse the buffering and newline logic in DelimitedCellGrabber
     * without rewriting it.
     * @param stream The underlying stream.
     * @return The "line grabber"
     */
    public static CellGrabber makeLineGrabber(InputStream stream) {
        final byte IllegalUtf8 = (byte)0xff;
        return new DelimitedCellGrabber(stream, IllegalUtf8, IllegalUtf8, true, false);
    }

    private final CellGrabber lineGrabber;
    private final int[] columnWidths;
    private final boolean ignoreSurroundingSpaces;
    private final boolean utf32CountingMode;
    private final ByteSlice rowText;
    private boolean needsUnderlyingRefresh;
    private int colIndex;
    private final MutableBoolean dummy;

    /** Constructor. */
    public FixedCellGrabber(final CellGrabber lineGrabber, final int[] columnWidths, boolean ignoreSurroundingSpaces,
                            boolean utf32CountingMode) {
        this.lineGrabber = lineGrabber;
        this.columnWidths = columnWidths;
        this.ignoreSurroundingSpaces = ignoreSurroundingSpaces;
        this.utf32CountingMode = utf32CountingMode;
        this.rowText = new ByteSlice();
        this.needsUnderlyingRefresh = true;
        this.colIndex = 0;
        this.dummy = new MutableBoolean();
    }

    @Override
    public void grabNext(ByteSlice dest, MutableBoolean lastInRow, MutableBoolean endOfInput) throws CsvReaderException {
        if (needsUnderlyingRefresh) {
            // Underlying row used up, and all columns provided. Ask underlying CellGrabber for the next line.
            lineGrabber.grabNext(rowText, dummy, endOfInput);

            if (endOfInput.booleanValue()) {
                // Set dest to the empty string, and leave 'endOfInput' set to true.
                dest.reset(rowText.data(), rowText.end(), rowText.end());
                return;
            }

            needsUnderlyingRefresh = false;
            colIndex = 0;
        }

        // There is data to return. Count off N characters. The final column gets all remaining characters.
        final boolean lastCol  = colIndex == columnWidths.length - 1;
        final int numCharsToTake = lastCol ? Integer.MAX_VALUE : columnWidths[colIndex];
        takeNCharactersInCharset(rowText, dest, numCharsToTake, utf32CountingMode);
        ++colIndex;
        needsUnderlyingRefresh = lastCol || dest.size() == 0;
        lastInRow.setValue(needsUnderlyingRefresh);
        endOfInput.setValue(false);

        if (ignoreSurroundingSpaces) {
            ReaderUtil.trimWhitespace(dest);
        }
    }

    private static void takeNCharactersInCharset(ByteSlice src, ByteSlice dest, int numCharsToTake,
                                                 boolean utf32CountingMode) {
        final byte[] data = src.data();
        final int cellBegin = src.begin();
        int current = cellBegin;
        while (numCharsToTake > 0) {
            if (current == src.end()) {
                break;
            }
            final int utf8Length = ReaderUtil.getUtf8Length(data[current]);
            final int charsConsumed = utf32CountingMode || utf8Length < 4 ? 1 : 2;
            numCharsToTake -= charsConsumed;
            if (numCharsToTake < 0) {
                throw new RuntimeException(
                        "While in UTF16 character counting mode, encountered a Unicode character outside the " +
                        "Basic Multilingual Plane. This requires two Java chars to represent, but the fixed field " +
                        "only has remainig space for one.");
            }
            current += utf8Length;
            if (current > src.end()) {
                throw new RuntimeException("Data error: partial UTF-8 sequence found in input");
            }
        }
        dest.reset(src.data(), cellBegin, current);
        src.reset(src.data(), current, src.end());
    }

    @Override
    public int physicalRowNum() {
        return lineGrabber.physicalRowNum();
    }
}
