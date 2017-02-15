package io.indexr.util;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Date;
import java.time.LocalDate;

public class DateTimeUtilTest {
    @Test
    public void dateTimeTest() {
        byte[] data = "2017-10-11".getBytes();
        long s = DateTimeUtil.parseDate(data, 0, data.length);
        Assert.assertEquals("2017-10-11", DateTimeUtil.getLocalDate(s).format(DateTimeUtil.DATE_FORMATTER));
        Assert.assertEquals("2017-10-11T00:00:00", DateTimeUtil.getLocalDateTime(s).format(DateTimeUtil.DATETIME_FORMATTER));

        data = "23:03:56".getBytes();
        s = DateTimeUtil.parseTime(data, 0, data.length);
        Assert.assertEquals("23:03:56", DateTimeUtil.getLocalTime(s).format(DateTimeUtil.TIME_FORMATTER));
        Assert.assertEquals("1970-01-01T23:03:56", DateTimeUtil.getLocalDateTime(s).format(DateTimeUtil.DATETIME_FORMATTER));

        data = "2017-10-11T23:03:56.348".getBytes();
        s = DateTimeUtil.parseDateTime(data, 0, data.length);
        Assert.assertEquals("2017-10-11T23:03:56.348", DateTimeUtil.getLocalDateTime(s).format(DateTimeUtil.DATETIME_FORMATTER));

        data = "2017-10-11 23:03:56".getBytes();
        s = DateTimeUtil.parseDateTime(data, 0, data.length);
        Assert.assertEquals("2017-10-11T23:03:56", DateTimeUtil.getLocalDateTime(s).format(DateTimeUtil.DATETIME_FORMATTER));

        Assert.assertEquals("1970-01-01T00:00:00", DateTimeUtil.getLocalDateTime(0).format(DateTimeUtil.DATETIME_FORMATTER));


        LocalDate date = LocalDate.now();
        Date sqlDate = Date.valueOf(date);
        long epochMillis = DateTimeUtil.getEpochMillisecond(sqlDate);
        Assert.assertEquals(date, DateTimeUtil.getLocalDate(epochMillis));
        Assert.assertEquals(sqlDate, DateTimeUtil.getJavaSQLDate(epochMillis));
    }

    @Test(expected = IllegalArgumentException.class)
    public void dateTimeFailTest() {
        byte[] data = "2017-10-".getBytes();
        DateTimeUtil.parseDate(data, 0, data.length);
    }
}
