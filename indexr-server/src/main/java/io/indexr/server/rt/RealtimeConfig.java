package io.indexr.server.rt;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import io.indexr.segment.SegmentMode;
import io.indexr.segment.rt.EventIgnoreStrategy;
import io.indexr.segment.rt.Fetcher;
import io.indexr.segment.rt.Metric;
import io.indexr.segment.rt.TagSetting;
import io.indexr.server.rt.fetcher.ConsoleFetcher;
import io.indexr.server.rt.fetcher.CsvFetcher;
import io.indexr.server.rt.fetcher.Kafka08Fetcher;
import io.indexr.server.rt.fetcher.TestFetcher;
import io.indexr.util.JsonUtil;

public class RealtimeConfig {
    private static Logger logger = LoggerFactory.getLogger(RealtimeConfig.class);

    static {
        SimpleModule fetcherModule = new SimpleModule("FetcherModule");

        // TODO put those logic into config.
        fetcherModule
                .addAbstractTypeMapping(Fetcher.class, Kafka08Fetcher.class)
                .registerSubtypes(new NamedType(Kafka08Fetcher.class, "kafka-0.8"))
                .addAbstractTypeMapping(Fetcher.class, TestFetcher.class)
                .registerSubtypes(new NamedType(TestFetcher.class, "test"))
                .addAbstractTypeMapping(Fetcher.class, ConsoleFetcher.class)
                .registerSubtypes(new NamedType(ConsoleFetcher.class, "console"))
                .addAbstractTypeMapping(Fetcher.class, CsvFetcher.class)
                .registerSubtypes(new NamedType(CsvFetcher.class, "csv"))
        ;
        JsonUtil.jsonMapper.registerModule(fetcherModule);
    }

    public static void loadSubtypes() {}

    @JsonProperty("dims")
    public List<String> dims;
    @JsonProperty("metrics")
    public List<Metric> metrics;
    @JsonProperty("name.alias")
    public Map<String, String> nameToAlias;
    @JsonProperty("tag.setting")
    public final TagSetting tagSetting;
    @JsonProperty("ignoreStrategy")
    public final EventIgnoreStrategy ignoreStrategy;
    @JsonProperty("grouping")
    public boolean grouping;
    @JsonProperty("save.period.minutes")
    public long savePeriodMinutes;
    @JsonProperty("upload.period.minutes")
    public long uploadPeriodMinutes;
    @JsonProperty("max.row.memory")
    public int maxRowInMemory;
    @JsonProperty("max.row.realtime")
    public long maxRowInRealtime;
    @JsonProperty("ingest")
    public boolean ingest;
    @JsonProperty("mode")
    public SegmentMode mode;
    @JsonProperty("fetcher")
    public Fetcher fetcher;

    public RealtimeConfig(@JsonProperty("dims") List<String> dims,
                          @JsonProperty("metrics") List<Metric> metrics,
                          @JsonProperty("name.alias") Map<String, String> nameToAlias,
                          @JsonProperty("tag.setting") TagSetting tagSetting,
                          @JsonProperty("ignoreStrategy") String ignoreStrategy,
                          @JsonProperty("grouping") Boolean grouping,
                          @JsonProperty("save.period.minutes") Long savePeriodMinutes,
                          @JsonProperty("upload.period.minutes") Long uploadPeriodMinutes,
                          @JsonProperty("max.row.memory") Integer maxRowInMemory,
                          @JsonProperty("max.row.realtime") Long maxRowInRealtime,
                          @JsonProperty("ingest") Boolean ingest,
                          @JsonProperty("compress") Boolean compress,
                          @JsonProperty("mode") String mode,
                          @JsonProperty("fetcher") Fetcher fetcher) {
        this.dims = dims;
        this.metrics = metrics;
        this.nameToAlias = nameToAlias;
        this.tagSetting = tagSetting;
        this.ignoreStrategy = EventIgnoreStrategy.fromName(ignoreStrategy);
        this.grouping = grouping == null ? true : grouping;
        this.savePeriodMinutes = savePeriodMinutes == null ? 20 : savePeriodMinutes;
        this.uploadPeriodMinutes = uploadPeriodMinutes == null ? 60 : uploadPeriodMinutes;
        this.maxRowInMemory = maxRowInMemory == null ? 500000 : maxRowInMemory;
        this.maxRowInRealtime = maxRowInRealtime == null ? 10000000 : maxRowInRealtime;
        this.ingest = ingest == null ? true : ingest;
        this.mode = SegmentMode.fromNameWithCompress(mode, compress);
        this.fetcher = fetcher;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RealtimeConfig that = (RealtimeConfig) o;

        if (grouping != that.grouping) return false;
        if (savePeriodMinutes != that.savePeriodMinutes) return false;
        if (uploadPeriodMinutes != that.uploadPeriodMinutes) return false;
        if (maxRowInMemory != that.maxRowInMemory) return false;
        if (maxRowInRealtime != that.maxRowInRealtime) return false;
        if (ingest != that.ingest) return false;
        if (mode != null ? !mode.equals(that.mode) : that.mode != null) return false;
        if (dims != null ? !dims.equals(that.dims) : that.dims != null) return false;
        if (metrics != null ? !metrics.equals(that.metrics) : that.metrics != null) return false;
        if (nameToAlias != null ? !nameToAlias.equals(that.nameToAlias) : that.nameToAlias != null)
            return false;
        if (tagSetting != null ? !tagSetting.equals(that.tagSetting) : that.tagSetting != null)
            return false;
        if (ignoreStrategy != null ? !ignoreStrategy.equals(that.ignoreStrategy) : that.ignoreStrategy != null)
            return false;
        return fetcher != null ? fetcher.equals(that.fetcher) : that.fetcher == null;

    }
}
