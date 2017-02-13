package io.indexr.util;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

public class DateTimeUtil {
    /**
     * Hours per day.
     */
    static final int HOURS_PER_DAY = 24;
    /**
     * Minutes per hour.
     */
    static final int MINUTES_PER_HOUR = 60;
    /**
     * Minutes per day.
     */
    static final int MINUTES_PER_DAY = MINUTES_PER_HOUR * HOURS_PER_DAY;
    /**
     * Seconds per minute.
     */
    static final int SECONDS_PER_MINUTE = 60;
    /**
     * Seconds per hour.
     */
    static final int SECONDS_PER_HOUR = SECONDS_PER_MINUTE * MINUTES_PER_HOUR;
    /**
     * Seconds per day.
     */
    static final int SECONDS_PER_DAY = SECONDS_PER_HOUR * HOURS_PER_DAY;
    /**
     * Milliseconds per day.
     */
    static final long MILLIS_PER_DAY = SECONDS_PER_DAY * 1000L;
    /**
     * The number of days in a 400 year cycle.
     */
    private static final int DAYS_PER_CYCLE = 146097;
    /**
     * The number of days from year zero to year 1970.
     * There are five 400 year cycles from year zero to 2000.
     * There are 7 leap years from 1970 to 2000.
     */
    private static final long DAYS_0000_TO_1970 = (DAYS_PER_CYCLE * 5L) - (30L * 365L + 7L);

    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;
    public static final DateTimeFormatter TIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2).toFormatter();
    public static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    public static LocalDate getLocalDate(long epochMillis) {
        long localEpochDay = Math.floorDiv(epochMillis, MILLIS_PER_DAY);
        return LocalDate.ofEpochDay(localEpochDay);
    }

    public static LocalTime getLocalTime(long epochMillis) {
        int msOfDay = (int) Math.floorMod(epochMillis, MILLIS_PER_DAY);
        return LocalTime.ofNanoOfDay(msOfDay * 1000_000L);
    }

    public static LocalDateTime getLocalDateTime(long epochMillis) {
        long localEpochDay = Math.floorDiv(epochMillis, MILLIS_PER_DAY);
        int msOfDay = (int) Math.floorMod(epochMillis, MILLIS_PER_DAY);
        LocalDate date = LocalDate.ofEpochDay(localEpochDay);
        LocalTime time = LocalTime.ofNanoOfDay(msOfDay * 1000_000L);
        return LocalDateTime.of(date, time);
    }

    public static Date getJavaSQLDate(long epochMillis) {
        LocalDate dateTime = getLocalDate(epochMillis);
        return Date.valueOf(dateTime);
    }

    public static Timestamp getJavaSQLTimeStamp(long epochMillis) {
        LocalDateTime dateTime = getLocalDateTime(epochMillis);
        return Timestamp.valueOf(dateTime);
    }

    public static long getEpochMillisecond(Date date) {
        long epochDay = date.toLocalDate().toEpochDay();
        return epochDay * MILLIS_PER_DAY;
    }

    public static long getEpochMillisecond(Timestamp timestamp) {
        return timestamp.toLocalDateTime().toEpochSecond(ZoneOffset.UTC) * 1000 + timestamp.getNanos() / 1000_1000L;
    }

    public static long parseDate(byte[] data) {
        return parseDate(data, 0, data.length);
    }

    /**
     * "2016-12-03", with "00:00:00".
     *
     * @return epoch seconds, -1 if failed.
     */
    public static long parseDate(byte[] data, int offset, int len) {
        int[] slice = _parseDate(data, offset, len);
        int year = slice[0];
        int month = slice[1];
        int day = slice[2];
        long epochDay = toEpochDay(year, month, day);
        return epochDay * SECONDS_PER_DAY * 1000;
    }

    private static int[] _parseDate(byte[] data, int offset, int len) {
        int[] slice = splitInts(3, 3, '-', data, offset, len);
        if (slice == null) {
            throw new IllegalArgumentException("Illegal DATE format: " + UTF8Util.fromUtf8(data, offset, len));
        }
        return slice;
    }

    public static int parseTime(byte[] data) {
        return parseTime(data, 0, data.length);
    }

    /**
     * "12:04:24.231", on "1970-01-01".
     */
    public static int parseTime(byte[] data, int offset, int len) {
        int[] slice = _parsetTime(data, offset, len);
        int hour = slice[0];
        int minute = slice[1];
        int second = slice[2];
        int millisecond = slice[3];
        return (hour * SECONDS_PER_HOUR + minute * SECONDS_PER_MINUTE + second) * 1000 + millisecond;
    }

    private static int[] _parsetTime(byte[] data, int offset, int len) {
        int timeOffset = offset;
        int timeLen = len;

        int millisecond = 0;
        int dot = search(data, timeOffset, timeLen, '.');
        if (dot >= 0) {
            timeLen = dot - timeOffset;

            int msOffset = dot + 1;
            int msLen = (offset + len) - (dot + 1);
            millisecond = (int) UTF8Util.parseLong(data, msOffset, msLen);
        }

        int[] timeSlice = splitInts(4, 3, ':', data, timeOffset, timeLen);
        if (timeSlice == null) {
            throw new IllegalArgumentException("Illegal TIME format: " + UTF8Util.fromUtf8(data, offset, len));
        }

        timeSlice[3] = millisecond;
        return timeSlice;
    }

    public static long parseDateTime(byte[] data) {
        return parseDateTime(data, 0, data.length);
    }

    /**
     * "2015-12-30 22:55:55" or "2015-12-30T22:55:55".
     */
    public static long parseDateTime(byte[] data, int offset, int len) {
        int space = search(data, offset, len, ' ', 'T');
        if (space == -1) {
            throw new IllegalArgumentException("Illegal DATETIME format: " + UTF8Util.fromUtf8(data, offset, len));
        }
        int dateOffset = offset;
        int dateLen = space - offset;
        int[] dateSlice = _parseDate(data, dateOffset, dateLen);
        int year = dateSlice[0];
        int month = dateSlice[1];
        int day = dateSlice[2];

        int timeOffset = space + 1;
        int timeLen = (offset + len) - (space + 1);
        int[] timeSlice = _parsetTime(data, timeOffset, timeLen);
        int hour = timeSlice[0];
        int minute = timeSlice[1];
        int second = timeSlice[2];
        int millisecond = timeSlice[3];

        long dateEpoch = toEpochDay(year, month, day) * SECONDS_PER_DAY * 1000;
        long timeEpoch = (hour * SECONDS_PER_HOUR + minute * SECONDS_PER_MINUTE + second) * 1000 + millisecond;
        return dateEpoch + timeEpoch;
    }

    private static int search(byte[] data, int offset, int len, char... chars) {
        for (int i = offset; i < offset + len; i++) {
            byte b = data[i];
            for (char c : chars) {
                if (c == b) {
                    return i;
                }
            }
        }
        return -1;
    }

    private static int[] splitInts(int arraySize, int expectSize, char split, byte[] data, int offset, int len) {
        int[] slice = new int[arraySize];
        int count = 0;
        int p1 = 0, p2 = 0;
        while (true) {
            if (p2 >= len || data[offset + p2] == split) {
                if (count >= expectSize) {
                    return null;
                }
                slice[count] = (int) UTF8Util.parseLong(data, offset + p1, p2 - p1);
                count++;
                p1 = p2 + 1;
                p2 = p1;

                if (p2 >= len) {
                    break;
                }
            } else {
                p2++;
            }
        }

        if (count != expectSize) {
            return null;
        }
        return slice;
    }

    // This code is copied from java.time package

    private static long toEpochDay(int year, int month, int day) {
        long y = year;
        long m = month;
        long total = 0;
        total += 365 * y;
        if (y >= 0) {
            total += (y + 3) / 4 - (y + 99) / 100 + (y + 399) / 400;
        } else {
            total -= y / -4 - y / -100 + y / -400;
        }
        total += ((367 * m - 362) / 12);
        total += day - 1;
        if (m > 2) {
            total--;
            if (!isLeapYear(year)) {
                total--;
            }
        }
        return total - DAYS_0000_TO_1970;
    }

    /**
     * Checks if the year is a leap year, according to the ISO proleptic
     * calendar system rules.
     */
    private static boolean isLeapYear(int prolepticYear) {
        return ((prolepticYear & 3) == 0) && ((prolepticYear % 100) != 0 || (prolepticYear % 400) == 0);
    }
}
