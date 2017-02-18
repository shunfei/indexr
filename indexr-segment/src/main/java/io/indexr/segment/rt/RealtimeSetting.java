package io.indexr.segment.rt;

import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import io.indexr.segment.SegmentMode;
import io.indexr.segment.SegmentSchema;

public class RealtimeSetting {
    public final SegmentSchema schema;
    public final List<String> dims;
    public final List<Metric> metrics;
    public final Map<String, String> nameToAlias;
    public final TagSetting tagSetting;
    public final EventIgnoreStrategy ignoreStrategy;
    public final long savePeriodMS;
    public final long uploadPeriodMS;
    public final int maxRowInMemory;
    public final long maxRowInRealtime;
    public final TimeZone timeZone;
    public final boolean grouping;
    public final boolean ingest;
    public final SegmentMode mode;
    public final Fetcher fetcher;

    public RealtimeSetting(SegmentSchema schema,
                           List<String> dims,
                           List<Metric> metrics,
                           Map<String, String> nameToAlias,
                           TagSetting tagSetting,
                           EventIgnoreStrategy ignoreStrategy,
                           long savePeriodMS,
                           long uploadPeriodMS,
                           int maxRowInMemory,
                           long maxRowInRealtime,
                           TimeZone timeZone,
                           boolean grouping,
                           boolean ingest,
                           SegmentMode mode,
                           Fetcher fetcher) {
        this.schema = schema;
        this.dims = dims;
        this.metrics = metrics;
        this.nameToAlias = nameToAlias;
        this.tagSetting = tagSetting;
        this.ignoreStrategy = ignoreStrategy;
        this.savePeriodMS = savePeriodMS;
        this.uploadPeriodMS = uploadPeriodMS;
        this.maxRowInMemory = maxRowInMemory;
        this.maxRowInRealtime = maxRowInRealtime;
        this.timeZone = timeZone;
        this.grouping = grouping;
        this.ingest = ingest;
        this.mode = mode;
        this.fetcher = fetcher;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RealtimeSetting that = (RealtimeSetting) o;

        if (savePeriodMS != that.savePeriodMS) return false;
        if (uploadPeriodMS != that.uploadPeriodMS) return false;
        if (maxRowInMemory != that.maxRowInMemory) return false;
        if (maxRowInRealtime != that.maxRowInRealtime) return false;
        if (grouping != that.grouping) return false;
        if (ingest != that.ingest) return false;
        if (mode != null ? !mode.equals(that.mode) : that.mode != null) return false;
        if (schema != null ? !schema.equals(that.schema) : that.schema != null) return false;
        if (dims != null ? !dims.equals(that.dims) : that.dims != null) return false;
        if (metrics != null ? !metrics.equals(that.metrics) : that.metrics != null) return false;
        if (nameToAlias != null ? !nameToAlias.equals(that.nameToAlias) : that.nameToAlias != null)
            return false;
        if (tagSetting != null ? !tagSetting.equals(that.tagSetting) : that.tagSetting != null)
            return false;
        if (ignoreStrategy != null ? !ignoreStrategy.equals(that.ignoreStrategy) : that.ignoreStrategy != null)
            return false;
        if (timeZone != null ? !timeZone.equals(that.timeZone) : that.timeZone != null)
            return false;
        return fetcher != null ? fetcher.equals(that.fetcher) : that.fetcher == null;
    }

    @Override
    public String toString() {
        return String.format("save: %s minutes, " +
                        "upload: %s minutes, " +
                        "maxRowInMemory: %d, " +
                        "maxRowInRealtime: %d, " +
                        "grouping: %s" +
                        "ingest: %s, " +
                        "mode: %s, " +
                        "fecther: %s",
                savePeriodMS / 1000 / 60.0,
                uploadPeriodMS / 1000 / 60.0,
                maxRowInMemory,
                maxRowInRealtime,
                grouping,
                ingest,
                mode,
                fetcher);
    }


}
