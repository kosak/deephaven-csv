package io.deephaven.csv.reading.cells;

import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.reading.headers.HeaderUtil;
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
    private boolean needToRefill;
    private final ByteSlice rowText;
    private int colIndex;
    private int colOffset;
    private final MutableBoolean dummy;

    /** Constructor. */
    public FixedCellGrabber(final CellGrabber lineGrabber, final int[] columnWidths, boolean ignoreSurroundingSpaces) {
        this.lineGrabber = lineGrabber;
        this.columnWidths = columnWidths;
        this.ignoreSurroundingSpaces = ignoreSurroundingSpaces;
        this.needToRefill = true;
        this.rowText = new ByteSlice();
        this.colIndex = 0;
        this.colOffset = 0;
        this.dummy = new MutableBoolean();
    }

    @Override
    public void grabNext(ByteSlice dest, MutableBoolean lastInRow, MutableBoolean endOfInput) throws CsvReaderException {
        while (true) {
            if (needToRefill) {
                // Underlying row used up, and all columns provided. Ask underlying CellGrabber for the next line.
                lineGrabber.grabNext(rowText, dummy, endOfInput);

                if (endOfInput.booleanValue()) {
                    // Set dest to the empty string, and leave 'endOfInput' set to true.
                    dest.reset(rowText.data(), rowText.end(), rowText.end());
                    // Leave 'endOfInput' set to true and return
                    return;
                }

                colIndex = 0;
                colOffset = rowText.begin();
                // There is a new underlying input line, so restart the logic from the top.
                needToRefill = false;
            }

            // There is data to return.
            final int cellBegin = colOffset;
            final int cellEnd = Math.min(colOffset + columnWidths[colIndex], rowText.end());
            ++colIndex;
            colOffset = cellEnd;

            dest.reset(rowText.data(), cellBegin, cellEnd);
            needToRefill = cellEnd == rowText.end();
            lastInRow.setValue(needToRefill);
            endOfInput.setValue(false);

            if (ignoreSurroundingSpaces) {
                HeaderUtil.trimWhitespace(dest);
            }
            return;
        }
    }

    @Override
    public int physicalRowNum() {
        return lineGrabber.physicalRowNum();
    }
}
