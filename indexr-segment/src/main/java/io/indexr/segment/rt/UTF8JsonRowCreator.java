package io.indexr.segment.rt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.indexr.util.UTF8JsonDeserializer;
import io.indexr.util.UTF8Util;

public class UTF8JsonRowCreator implements UTF8JsonDeserializer.Listener {
    private static final Logger logger = LoggerFactory.getLogger(UTF8JsonRowCreator.class);
    private final UTF8JsonDeserializer utf8JsonDeserializer;

    private String creatorName;
    private UTF8Row.Creator creator;

    private List<UTF8Row> curRows;
    private byte[] curData;

    private long consumeCount = 0;
    private long produceCount = 0;
    private long ignoreCount = 0;
    private long failCount = 0;

    public UTF8JsonRowCreator(boolean numberEmptyAsZero) {
        this.utf8JsonDeserializer = new UTF8JsonDeserializer(numberEmptyAsZero);
    }

    public UTF8JsonRowCreator() {
        this(false);
    }

    public UTF8JsonRowCreator setRowCreator(String name, UTF8Row.Creator creator) {
        this.creatorName = name;
        this.creator = creator;
        return this;
    }

    public long getConsumeCount() {
        return consumeCount;
    }

    public long getProduceCount() {
        return produceCount;
    }

    public long getIgnoreCount() {
        return ignoreCount;
    }

    public long getFailCount() {
        return failCount;
    }

    public void resetStat() {
        consumeCount = 0;
        ignoreCount = 0;
        produceCount = 0;
        failCount = 0;
    }

    @Override
    public void onStartJson(int offset) {
        creator.startRow();
    }

    @Override
    public void onFinishJson(int offset, int len) {
        UTF8Row r = creator.endRow();
        if (r != null) {
            curRows.add(r);
            produceCount++;
        } else {
            ignoreCount++;
            if (logger.isTraceEnabled()) {
                logger.trace("creator [{}] ignore event: {}", creatorName, UTF8Util.fromUtf8(curData, offset, len));
            }
        }
    }

    @Override
    public byte onKey(ByteBuffer key, int size) {
        return creator.onColumnUTF8Name(key, size);
    }

    @Override
    public boolean onStringValue(ByteBuffer value, int size) {
        return creator.onStringValue(value, size);
    }

    @Override
    public boolean onIntValue(int value) {
        return creator.onIntValue(value);
    }

    @Override
    public boolean onLongValue(long value) {
        return creator.onLongValue(value);
    }

    @Override
    public boolean onFloatValue(float value) {
        return creator.onFloatValue(value);
    }

    @Override
    public boolean onDoubleValue(double value) {
        return creator.onDoubleValue(value);
    }

    public List<UTF8Row> create(byte[] jsonBytes) {
        return create(jsonBytes, 0, jsonBytes.length);
    }

    public List<UTF8Row> create(byte[] jsonBytes, int offset, int len) {
        consumeCount++;
        curData = jsonBytes;
        curRows = new ArrayList<>();

        boolean ok = utf8JsonDeserializer.parse(jsonBytes, offset, len, this);
        if (!ok) {
            failCount++;
            if (logger.isTraceEnabled()) {
                logger.trace("creator [{}] found illegal event: {}", creatorName, UTF8Util.fromUtf8(jsonBytes));
            }
            for (UTF8Row row : curRows) {
                row.free();
            }
            curData = null;
            curRows = null;
            return Collections.emptyList();
        }
        List<UTF8Row> rt = curRows;

        curData = null;
        curRows = null;
        return rt;
    }
}