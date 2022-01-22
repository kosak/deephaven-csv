package io.deephaven.csv.benchmark.util;

import java.time.ZonedDateTime;

public final class DateTimeParser {
    public static long parseDateTime(final String dateText) {
        final ZonedDateTime zdt = ZonedDateTime.parse(dateText);
        final long zdtSeconds = zdt.toEpochSecond();
        final int zdtNanos = zdt.getNano();
        return zdtSeconds * 1_000_000_000 + zdtNanos;
    }
}
