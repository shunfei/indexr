package io.indexr.hive;

import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Date;
import java.time.LocalDate;

import io.indexr.util.DateTimeUtil;

public class HiveDateTimeTest {
    @Test
    public void testDateTime() {
        LocalDate date = LocalDate.now();
        Date sqlDate = Date.valueOf(date);
        long epochMillis = DateTimeUtil.getEpochMillisecond(sqlDate);
        Assert.assertEquals(date, DateTimeUtil.getLocalDate(epochMillis));
        Assert.assertEquals(sqlDate, DateTimeUtil.getJavaSQLDate(epochMillis));

        DateWritable dateWritable = new DateWritable(DateTimeUtil.getJavaSQLDate(epochMillis));
        Assert.assertEquals(epochMillis, DateTimeUtil.getEpochMillisecond(dateWritable.get()));
    }
}
