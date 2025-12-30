package io.deephaven.csv.reading;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.densestorage.DenseStorageReader;
import io.deephaven.csv.parsers.*;
import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.sinks.SinkFactory;
import io.deephaven.csv.tokenization.Tokenizer;
import io.deephaven.csv.util.*;

import java.util.*;

import org.jetbrains.annotations.NotNull;

/**
 * The job of this class is to take a column of cell text, as prepared by {@link ParseInputToDenseStorage}, do type
 * inference if appropriate, and parse the text into typed data.
 */
public final class ParseDenseStorageToColumn {
    /**
     * @param colNum The column number being parsed. Some custom sinks use this for their own information.
     * @param dsr A reader for the input.
     * @param parsers The set of parsers to try. If null, then {@link Parsers#DEFAULT} will be used.
     * @param specs The CsvSpecs which control how the column is interpreted.
     * @param nullValueLiteralsToUse If a cell text is equal to any of the values in this array, the cell will be
     *        interpreted as the null value. Typically set to a one-element array containing the empty string.
     * @param sinkFactory Factory that makes all the Sinks of various types, used to consume the data we produce.
     * @return The {@link Sink}, provided by the caller's {@link SinkFactory}, that was selected to hold the column
     *         data.
     * @throws CsvReaderException If there is an error processing the input.
     */
    public static Result doit(
            final int colNum,
            final Moveable<DenseStorageReader> dsr,
            final List<Parser<?>> parsers,
            final CsvSpecs specs,
            final String[] nullValueLiteralsToUse,
            final SinkFactory sinkFactory)
            throws CsvReaderException {
        // Canonicalize the parsers (remove duplicates) but preserve the order.
        Set<Parser<?>> parserSet = new LinkedHashSet<>(parsers != null ? parsers : Parsers.DEFAULT);

        final Tokenizer tokenizer = new Tokenizer(specs.customDoubleParser(), specs.customTimeZoneParser());
        final Parser.GlobalContext gctx =
                new Parser.GlobalContext(colNum, tokenizer, sinkFactory, nullValueLiteralsToUse);

        // Make two IteratorHolders for (potentially) having two passes over the input. We take care to not hold these
        // references longer than necessary, to give the GC a chance to collect the data in our linked list.
        final Moveable<IteratorHolder> ihAlt = new Moveable<>(new IteratorHolder(dsr.get().copy()));
        final Moveable<IteratorHolder> ih = new Moveable<>(new IteratorHolder(dsr.move().get()));

        // Skip over leading null cells. There are four cases:
        // 1. The column is empty. In this case we run the "empty parser"
        // 2. There is only one available parser. In this case we shortcut to that parser and let it deal with the
        // column, whether full of nulls or not. (Doing this early helps certain use cases, such as Deephaven
        // Enterprise, in which column sinks synchronize each other and so it is harmful if one column gets too far
        // ahead without writing to its sink, as would happen in our null-skipping type inference logic).
        // 3. The column is full of all nulls
        // 4. There is a non-null cell (so the type inference process can begin)

        final Parser<?> nullParserToUse =
                parserSet.size() == 1 ? parserSet.iterator().next() : specs.nullParser();

        if (!ih.get().tryMoveNext()) {
            // Case 1: The column is empty
            if (nullParserToUse == null) {
                throw new CsvReaderException(
                        "Column is empty, so can't infer type of column, and nullParser is not specified.");
            }
            ih.reset();
            ihAlt.reset();
            return emptyParse(nullParserToUse, gctx);
        }

        if (parserSet.size() == 1) {
            // Case 2. There is only one available parser.
            final Parser<?> parserToUse = parserSet.iterator().next();
            ih.reset();
            return onePhaseParse(parserToUse, gctx, ihAlt.move());
        }

        boolean columnIsAllNulls = true;
        do {
            if (!gctx.isNullCell(ih.get())) {
                columnIsAllNulls = false;
                break;
            }
        } while (ih.get().tryMoveNext());

        if (columnIsAllNulls) {
            // case 3. The column is full of all nulls
            if (nullParserToUse == null) {
                throw new CsvReaderException(
                        "Column contains all null cells, so can't infer type of column, and nullParser is not specified.");
            }
            ih.reset();
            return onePhaseParse(nullParserToUse, gctx, ihAlt.move());
        }

        // The rest of this logic is for case 2: there is a non-null cell (so the type inference process can begin).

        final CategorizedParsers cats = CategorizedParsers.create(parserSet);

        // Numerics are special and they get their own fast path that uses Sources and Sinks rather than
        // reparsing the text input.
        final MutableDouble dummyDouble = new MutableDouble();
        if (!cats.numericParsers.isEmpty() && tokenizer.tryParseDouble(ih.get().bs(), dummyDouble)) {
            return parseNumerics(cats, gctx, ih.move(), ihAlt.move());
        }

        List<Parser<?>> parsersBeforeCustom = Collections.emptyList();
        final MutableBoolean dummyBoolean = new MutableBoolean();
        final MutableLong dummyLong = new MutableLong();
        if (cats.timestampParser != null && tokenizer.tryParseLong(ih.get().bs(), dummyLong)) {
            parsersBeforeCustom = Collections.singletonList(cats.timestampParser);
        } else if (cats.booleanParser != null && tokenizer.tryParseBoolean(ih.get().bs(), dummyBoolean)) {
            parsersBeforeCustom = Collections.singletonList(cats.booleanParser);
        } else if (cats.dateTimeParser != null && tokenizer.tryParseDateTime(ih.get().bs(), dummyLong)) {
            parsersBeforeCustom = Collections.singletonList(cats.dateTimeParser);
        }
        return parseFromCuratedSelections(parsersBeforeCustom,
                cats.customParsers,
                cats.charAndStringParsers,
                gctx, ih.move(), ihAlt.move());
    }

    @NotNull
    private static Result parseNumerics(CategorizedParsers cats, final Parser.GlobalContext gctx,
            Moveable<IteratorHolder> ih, Moveable<IteratorHolder> ihAlt) throws CsvReaderException {
        final List<ParserResultWrapper<?>> wrappers = new ArrayList<>();
        for (Parser<?> parser : cats.numericParsers) {
            final ParserResultWrapper<?> prw = parseNumericsHelper(parser, gctx, ih.get());
            wrappers.add(prw);
            if (ih.get().isExhausted()) {
                break;
            }
        }

        if (!ih.get().isExhausted()) {
            if (cats.customParsers.isEmpty() && cats.charAndStringParsers.isEmpty()) {
                final String message = String.format(
                        "Consumed %d numeric items, then encountered a non-numeric item but there are no custom or char/string parsers available.",
                        ih.get().numConsumed() - 1);
                throw new CsvReaderException(message);
            }
            // Tried all numeric parsers but couldn't consume all input. Fall back to the custom parsers (if any),
            // then char and string parsers.
            wrappers.clear();
            return parseFromCuratedSelections(new ArrayList<>(), cats.customParsers, cats.charAndStringParsers,
                    gctx, ih.move(), ihAlt.move());
        }

        ih.reset();

        // If all the wrappers implement the Source interface (except possibly the last, which doesn't need to),
        // we can read the data back and cast it to the right numeric type.
        if (canUnify(wrappers)) {
            ihAlt.reset();
            return unifyNumericResults(gctx, wrappers);
        }
        // Otherwise (if some wrappers do not implement the Source interface), we have to do a reparse.
        final ParserResultWrapper<?> last = wrappers.get(wrappers.size() - 1);
        return performSecondParsePhase(gctx, last, ihAlt.get());
    }

    private static boolean canUnify(final List<ParserResultWrapper<?>> items) {
        for (int i = 0; i < items.size() - 1; ++i) {
            if (items.get(i).pctx.source() == null) {
                return false;
            }
        }
        return true;
    }

    @NotNull
    private static <TARRAY> ParserResultWrapper<TARRAY> parseNumericsHelper(
            Parser<TARRAY> parser, final Parser.GlobalContext gctx, final IteratorHolder ih)
            throws CsvReaderException {
        final Parser.ParserContext<TARRAY> pctx = parser.makeParserContext(gctx, Parser.CHUNK_SIZE);
        final long begin = ih.numConsumed() - 1;
        final long end = parser.tryParse(gctx, pctx, ih, begin, Long.MAX_VALUE, true);
        return new ParserResultWrapper<>(parser, pctx, begin, end);
    }

    @NotNull
    private static Result parseFromCuratedSelections(
            final List<Parser<?>> parsersBeforeCustom,
            final List<Parser<?>> customParsers,
            final List<Parser<?>> parsersAfterCustom,
            final Parser.GlobalContext gctx,
            Moveable<IteratorHolder> ih,
            Moveable<IteratorHolder> ihAlt) throws CsvReaderException {
        List<Parser<?>> parsers = new ArrayList<>();
        parsers.addAll(parsersBeforeCustom);
        final int customBegin = parsers.size();
        parsers.addAll(customParsers);
        final int customEnd = parsers.size();
        parsers.addAll(parsersAfterCustom);

        if (parsers.isEmpty()) {
            throw new CsvReaderException("No available parsers.");
        }

        for (int ii = 0; ii < parsers.size() - 1; ++ii) {
            final Result result;
            if (ii < customBegin || ii >= customEnd) {
                result = tryTwoPhaseParse(parsers.get(ii), gctx, ih.get(), ihAlt.get());
            } else {
                final IteratorHolder tempFullIterator = new IteratorHolder(ihAlt.get().dsr().copy());
                tempFullIterator.tryMoveNext(); // Input is not empty, so we know this will succeed.
                result = tryTwoPhaseParse(parsers.get(ii), gctx, tempFullIterator, ihAlt.get());
            }
            if (result != null) {
                return result;
            }
        }

        // The final parser in the set gets special (more efficient) handling because there's nothing to
        // fall back to.
        ih.reset();
        return onePhaseParse(parsers.get(parsers.size() - 1), gctx, ihAlt.move());
    }

    private static <TARRAY> Result tryTwoPhaseParse(
            final Parser<TARRAY> parser,
            final Parser.GlobalContext gctx,
            final IteratorHolder ih,
            final IteratorHolder ihAlt) throws CsvReaderException {
        final long phaseOneStart = ih.numConsumed() - 1;
        final Parser.ParserContext<TARRAY> pctx = parser.makeParserContext(gctx, Parser.CHUNK_SIZE);
        final long end = parser.tryParse(gctx, pctx, ih, phaseOneStart, Long.MAX_VALUE, true);
        if (!ih.isExhausted()) {
            // This parser couldn't make it to the end but there are others remaining to try. Signal a
            // failure to the caller so that it can try the next one. Note that 'ih' has been advanced
            // to the failing entry and 'ihAlt' has not been modified.
            return null;
        }
        if (phaseOneStart == 0) {
            // Reached end, and started at zero so everything was parsed and we are done.
            return new Result(pctx.sink(), pctx.dataType());
        }

        // Reached end but started somewhere other than zero. We will do the second phase parse
        // now. By the assumption of the algorithm, the second phase parse will not fail with a
        // parse fail. (If it does, we will throw an exception).
        final ParserResultWrapper<TARRAY> wrapper = new ParserResultWrapper<>(parser, pctx, phaseOneStart, end);
        return performSecondParsePhase(gctx, wrapper, ihAlt);
    }

    private static <TARRAY> Result performSecondParsePhase(final Parser.GlobalContext gctx,
            final ParserResultWrapper<TARRAY> wrapper, final IteratorHolder ihAlt) throws CsvReaderException {
        ihAlt.tryMoveNext(); // Input is not empty, so we know this will succeed.
        final long end = wrapper.parser.tryParse(gctx, wrapper.pctx, ihAlt, 0, wrapper.begin, false);

        if (end == wrapper.begin) {
            return new Result(wrapper.pctx.sink(), wrapper.pctx.dataType());
        }
        final String message = "Logic error: second parser phase failed on input. Parser was: "
                + wrapper.parser.getClass().getCanonicalName();
        throw new RuntimeException(message);
    }

    @NotNull
    private static <TARRAY> Result onePhaseParse(final Parser<TARRAY> parser, final Parser.GlobalContext gctx,
            final Moveable<IteratorHolder> ihAlt) throws CsvReaderException {
        final Parser.ParserContext<TARRAY> pctx = parser.makeParserContext(gctx, Parser.CHUNK_SIZE);
        ihAlt.get().tryMoveNext(); // Input is not empty, so we know this will succeed.
        parser.tryParse(gctx, pctx, ihAlt.get(), 0, Long.MAX_VALUE, true);
        if (ihAlt.get().isExhausted()) {
            return new Result(pctx.sink(), pctx.dataType());
        }
        final String message = String.format(
                "Parsing failed on input, with nothing left to fall back to. Parser %s successfully parsed %d items before failure.",
                parser.getClass().getCanonicalName(), ihAlt.get().numConsumed() - 1);
        throw new CsvReaderException(message);
    }

    @NotNull
    private static <TARRAY> Result emptyParse(
            final Parser<TARRAY> parser, final Parser.GlobalContext gctx) throws CsvReaderException {
        // The parser won't do any "parsing" here, but it will create a Sink.
        final Parser.ParserContext<TARRAY> pctx = parser.makeParserContext(gctx, Parser.CHUNK_SIZE);
        parser.tryParse(gctx, pctx, null, 0, 0, true); // Result ignored.
        return new Result(pctx.sink(), pctx.dataType());
    }

    @NotNull
    private static Result unifyNumericResults(
            final Parser.GlobalContext gctx, final List<ParserResultWrapper<?>> wrappers) {
        if (wrappers.isEmpty()) {
            throw new RuntimeException("Logic error: no parser results.");
        }
        final ParserResultWrapper<?> dest = wrappers.get(wrappers.size() - 1);

        // BTW, there's an edge case where there's only one parser in the list. In that case
        // first == dest, but this code still does the right thing.
        final ParserResultWrapper<?> first = wrappers.get(0);
        fillNulls(gctx, dest.pctx, 0, first.begin);

        long destBegin = first.begin;
        for (int ii = 0; ii < wrappers.size() - 1; ++ii) {
            final ParserResultWrapper<?> curr = wrappers.get(ii);
            copy(gctx, curr.pctx, dest.pctx, curr.begin, curr.end, destBegin);
            destBegin += (curr.end - curr.begin);
        }
        return new Result(dest.pctx.sink(), dest.pctx.dataType());
    }

    private static <TARRAY, UARRAY> void copy(
            final Parser.GlobalContext gctx,
            final Parser.ParserContext<TARRAY> sourceCtx,
            final Parser.ParserContext<UARRAY> destCtx,
            final long srcBegin,
            final long srcEnd,
            final long destBegin) {
        TypeConverter.copy(
                sourceCtx.source(),
                destCtx.sink(),
                srcBegin,
                srcEnd,
                destBegin,
                sourceCtx.valueChunk(),
                destCtx.valueChunk(),
                gctx.nullChunk());
    }

    private static <TARRAY> void fillNulls(
            final Parser.GlobalContext gctx,
            final Parser.ParserContext<TARRAY> pctx,
            final long begin,
            final long end) {
        if (begin == end) {
            return;
        }
        final boolean[] nullBuffer = gctx.nullChunk();
        final Sink<TARRAY> destSink = pctx.sink();
        final TARRAY values = pctx.valueChunk();

        final int sizeToInit = Math.min(nullBuffer.length, Math.toIntExact(end - begin));
        Arrays.fill(nullBuffer, 0, sizeToInit, true);

        for (long current = begin; current != end;) { // no ++
            final long endToUse = Math.min(current + nullBuffer.length, end);
            // Don't care about the actual values, only the null flag values (which are all true).
            destSink.write(values, nullBuffer, current, endToUse, false);
            current = endToUse;
        }
    }

    private static <T> List<T> limitToSpecified(Collection<T> items, Set<T> limitTo) {
        final List<T> result = new ArrayList<>();
        for (T item : items) {
            if (limitTo.contains(item)) {
                result.add(item);
            }
        }
        return result;
    }

    /**
     * The result type, containing the Sink that was chosen to hold the data, and the DataType indicating the type of
     * that data.
     */
    public static class Result {
        private final Sink<?> sink;
        private final DataType dataType;

        /**
         * Constructor.
         * 
         * @param sink The Sink that was chosen to hold the data.
         * @param dataType The DataType of the data.
         */
        public Result(Sink<?> sink, DataType dataType) {
            this.sink = sink;
            this.dataType = dataType;
        }

        /**
         * Gets the Sink that was chosen to hold the data
         * 
         * @return The Sink
         */
        public Sink<?> sink() {
            return sink;
        }

        /**
         * Gets the DataType of the data.
         * 
         * @return The DataType of the data.
         */
        public DataType dataType() {
            return dataType;
        }
    }

    private static class CategorizedParsers {
        public static CategorizedParsers create(final Collection<Parser<?>> parsers)
                throws CsvReaderException {
            Parser<?> booleanParser = null;
            final Set<Parser<?>> specifiedNumericParsers = new HashSet<>();
            // Subset of the above.
            final List<Parser<?>> specifiedFloatingPointParsers = new ArrayList<>();
            Parser<?> dateTimeParser = null;
            final Set<Parser<?>> specifiedCharAndStringParsers = new HashSet<>();
            final List<Parser<?>> specifiedTimeStampParsers = new ArrayList<>();
            final List<Parser<?>> specifiedCustomParsers = new ArrayList<>();
            for (Parser<?> p : parsers) {
                if (p == Parsers.BYTE || p == Parsers.SHORT || p == Parsers.INT || p == Parsers.LONG) {
                    specifiedNumericParsers.add(p);
                    continue;
                }

                if (p == Parsers.FLOAT_FAST || p == Parsers.FLOAT_STRICT || p == Parsers.DOUBLE) {
                    specifiedNumericParsers.add(p);
                    specifiedFloatingPointParsers.add(p);
                    continue;
                }

                if (p == Parsers.TIMESTAMP_SECONDS
                        || p == Parsers.TIMESTAMP_MILLIS
                        || p == Parsers.TIMESTAMP_MICROS
                        || p == Parsers.TIMESTAMP_NANOS) {
                    specifiedTimeStampParsers.add(p);
                    continue;
                }

                if (p == Parsers.CHAR || p == Parsers.STRING) {
                    specifiedCharAndStringParsers.add(p);
                    continue;
                }

                if (p == Parsers.BOOLEAN) {
                    booleanParser = p;
                    continue;
                }

                if (p == Parsers.DATETIME) {
                    dateTimeParser = p;
                    continue;
                }

                specifiedCustomParsers.add(p);
            }

            if (specifiedFloatingPointParsers.size() > 1) {
                throw new CsvReaderException(
                        "There is more than one floating point parser in the parser set.");
            }

            if (specifiedTimeStampParsers.size() > 1) {
                throw new CsvReaderException("There is more than one timestamp parser in the parser set.");
            }

            if (!specifiedNumericParsers.isEmpty() && !specifiedTimeStampParsers.isEmpty()) {
                throw new CsvReaderException(
                        "The parser set must not contain both numeric and timestamp parsers.");
            }

            final List<Parser<?>> allNumericParsersByPrecedence =
                    Arrays.asList(
                            Parsers.BYTE,
                            Parsers.SHORT,
                            Parsers.INT,
                            Parsers.LONG,
                            Parsers.FLOAT_FAST,
                            Parsers.FLOAT_STRICT,
                            Parsers.DOUBLE);
            final List<Parser<?>> allCharAndStringParsersByPrecedence =
                    Arrays.asList(Parsers.CHAR, Parsers.STRING);

            final List<Parser<?>> numericParsers =
                    limitToSpecified(allNumericParsersByPrecedence, specifiedNumericParsers);
            final List<Parser<?>> charAndStringParsers =
                    limitToSpecified(allCharAndStringParsersByPrecedence, specifiedCharAndStringParsers);
            final Parser<?> timestampParser =
                    specifiedTimeStampParsers.isEmpty() ? null : specifiedTimeStampParsers.get(0);

            return new CategorizedParsers(
                    booleanParser,
                    numericParsers,
                    dateTimeParser,
                    charAndStringParsers,
                    timestampParser,
                    specifiedCustomParsers);
        }

        private final Parser<?> booleanParser;
        private final List<Parser<?>> numericParsers;
        private final Parser<?> dateTimeParser;
        private final List<Parser<?>> charAndStringParsers;
        private final Parser<?> timestampParser;
        private final List<Parser<?>> customParsers;

        private CategorizedParsers(
                Parser<?> booleanParser,
                List<Parser<?>> numericParsers,
                Parser<?> dateTimeParser,
                List<Parser<?>> charAndStringParsers,
                Parser<?> timestampParser,
                List<Parser<?>> customParsers) {
            this.booleanParser = booleanParser;
            this.numericParsers = numericParsers;
            this.dateTimeParser = dateTimeParser;
            this.charAndStringParsers = charAndStringParsers;
            this.timestampParser = timestampParser;
            this.customParsers = customParsers;
        }
    }

    private static class ParserResultWrapper<TARRAY> {
        private final Parser<TARRAY> parser;
        private final Parser.ParserContext<TARRAY> pctx;
        private final long begin;
        private final long end;

        public ParserResultWrapper(Parser<TARRAY> parser, Parser.ParserContext<TARRAY> pctx, long begin, long end) {
            this.parser = parser;
            this.pctx = pctx;
            this.begin = begin;
            this.end = end;
        }
    }
}
