package io.indexr.server.rt.fetcher;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.directory.api.util.Strings;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Random;

import io.indexr.segment.ColumnSchema;
import io.indexr.segment.ColumnType;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.rt.Fetcher;
import io.indexr.segment.rt.UTF8JsonRowCreator;
import io.indexr.segment.rt.UTF8Row;

public class TestFetcher implements Fetcher {
    private static final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHH");

    @JsonProperty("sleep")
    public final long sleep;
    @JsonProperty("random")
    public final boolean randomValue;
    @JsonProperty("tag.field")
    public final String tagField;
    @JsonProperty("random.tags")
    public final List<String> randomTags;


    private SegmentSchema schema;
    private Random random = new Random();

    private final UTF8JsonRowCreator utf8JsonRowCreator = new UTF8JsonRowCreator(true);
    private volatile boolean closed = false;

    //private static long v = 0;

    @JsonCreator
    public TestFetcher(@JsonProperty("sleep") long sleep,
                       @JsonProperty("random") boolean randomValue,
                       @JsonProperty("tag.field") String tagField,
                       @JsonProperty("random.tags") List<String> randomTags) {
        this.sleep = sleep;
        this.randomValue = randomValue;
        this.tagField = tagField;
        this.randomTags = randomTags;
    }

    @Override
    public void setRowCreator(String name, UTF8Row.Creator rowCreator) {
        utf8JsonRowCreator.setRowCreator(name, rowCreator);
    }

    @Override
    public boolean ensure(SegmentSchema schema) throws Exception {
        this.schema = schema;
        closed = false;
        return true;
    }

    @Override
    public boolean hasNext() throws Exception {
        return !closed;
    }

    @Override
    public List<UTF8Row> next() throws Exception {
        if (sleep > 0) {
            Thread.sleep(sleep);
        }

        int colId = 0;
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        if (!Strings.isEmpty(tagField) && randomTags != null && !randomTags.isEmpty()) {
            sb.append("\"").append(tagField).append("\":\"").append(randomTags.get(random.nextInt(randomTags.size()))).append("\",");
        }
        for (ColumnSchema cs : schema.getColumns()) {
            sb.append('\"').append(cs.getName()).append("\": ");
            if ("time".equals(cs.getName())) {
                ZonedDateTime now = ZonedDateTime.now();
                long calValue = Long.parseLong(now.format(timeFormatter));
                //long calValue = v++;
                sb.append(calValue);
            } else {
                if (randomValue) {
                    if (cs.getDataType() == ColumnType.STRING) {
                        String colValue = RandomStringUtils.randomAlphabetic(random.nextInt(20));
                        sb.append('\"').append(colValue).append('\"');
                    } else {
                        long colValue = random.nextInt();
                        sb.append(colValue);
                    }
                } else {
                    sb.append("\"1\"");
                }
            }

            colId++;
            if (colId < schema.getColumns().size()) {
                sb.append(',');
            }
        }
        sb.append('}');

        byte[] data = sb.toString().getBytes("utf-8");
        return utf8JsonRowCreator.create(data);
    }

    @Override
    public void commit() {
    }

    @Override
    public void close() throws IOException {
        closed = true;
    }

    @Override
    public boolean equals(Fetcher o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TestFetcher that = (TestFetcher) o;

        if (sleep != that.sleep) return false;
        if (randomValue != that.randomValue) return false;
        if (tagField != null ? !tagField.equals(that.tagField) : that.tagField != null)
            return false;
        return randomTags != null ? randomTags.equals(that.randomTags) : that.randomTags == null;

    }

    @Override
    public String toString() {
        return "TestFetcher{" +
                "sleep=" + sleep +
                ", randomValue=" + randomValue +
                '}';
    }

    @Override
    public long statConsume() {
        return utf8JsonRowCreator.getConsumeCount();
    }

    @Override
    public long statProduce() {
        return utf8JsonRowCreator.getProduceCount();
    }

    @Override
    public long statIgnore() {
        return utf8JsonRowCreator.getIgnoreCount();
    }

    @Override
    public long statFail() {
        return utf8JsonRowCreator.getFailCount();
    }

    @Override
    public void statReset() {
        utf8JsonRowCreator.resetStat();
    }
}
