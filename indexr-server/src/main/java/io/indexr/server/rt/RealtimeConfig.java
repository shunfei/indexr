package io.indexr.server.rt;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import io.indexr.segment.SegmentMode;
import io.indexr.segment.rt.AggSchema;
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

    @JsonIgnore
    public AggSchema aggSchema;
    @JsonProperty("name.alias")
    public final Map<String, String> nameToAlias;
    @JsonProperty("tag.setting")
    public final TagSetting tagSetting;
    @JsonProperty("ignoreStrategy")
    public final EventIgnoreStrategy ignoreStrategy;
    @JsonProperty("save.period.minutes")
    public final long savePeriodMinutes;
    @JsonProperty("upload.period.minutes")
    public final long uploadPeriodMinutes;
    @JsonProperty("max.row.memory")
    public final int maxRowInMemory;
    @JsonProperty("max.row.realtime")
    public final long maxRowInRealtime;
    @JsonProperty("ingest")
    public final boolean ingest;
    @JsonProperty("mode")
    public String modeName;
    @JsonIgnore
    public SegmentMode mode;
    @JsonProperty("fetcher")
    public final Fetcher fetcher;

    public RealtimeConfig(@JsonProperty("grouping") Boolean grouping,
                          @JsonProperty("dims") List<String> dims,
                          @JsonProperty("metrics") List<Metric> metrics,
                          @JsonProperty("name.alias") Map<String, String> nameToAlias,
                          @JsonProperty("tag.setting") TagSetting tagSetting,
                          @JsonProperty("ignoreStrategy") String ignoreStrategy,
                          @JsonProperty("save.period.minutes") Long savePeriodMinutes,
                          @JsonProperty("upload.period.minutes") Long uploadPeriodMinutes,
                          @JsonProperty("max.row.memory") Integer maxRowInMemory,
                          @JsonProperty("max.row.realtime") Long maxRowInRealtime,
                          @JsonProperty("ingest") Boolean ingest,
                          @JsonProperty("compress") Boolean compress,
                          @JsonProperty("mode") String modeName,
                          @JsonProperty("fetcher") Fetcher fetcher) {
        this.aggSchema = new AggSchema(grouping != null && grouping, dims, metrics);
        this.nameToAlias = nameToAlias;
        this.tagSetting = tagSetting;
        this.ignoreStrategy = EventIgnoreStrategy.fromName(ignoreStrategy);
        this.savePeriodMinutes = savePeriodMinutes == null ? 20 : savePeriodMinutes;
        this.uploadPeriodMinutes = uploadPeriodMinutes == null ? 60 : uploadPeriodMinutes;
        this.maxRowInMemory = maxRowInMemory == null ? 500000 : maxRowInMemory;
        this.maxRowInRealtime = maxRowInRealtime == null ? 10000000 : maxRowInRealtime;
        this.ingest = ingest == null ? true : ingest;
        this.mode = SegmentMode.fromNameWithCompress(modeName, compress);
        this.modeName = this.mode.name();
        this.fetcher = fetcher;
    }

    public void setAggSchema(AggSchema aggSchema) {
        this.aggSchema = aggSchema;
    }

    public void setMode(SegmentMode mode) {
        this.modeName = mode.name();
        this.mode = mode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RealtimeConfig that = (RealtimeConfig) o;

        if (savePeriodMinutes != that.savePeriodMinutes) return false;
        if (uploadPeriodMinutes != that.uploadPeriodMinutes) return false;
        if (maxRowInMemory != that.maxRowInMemory) return false;
        if (maxRowInRealtime != that.maxRowInRealtime) return false;
        if (ingest != that.ingest) return false;
        if (aggSchema != null ? !aggSchema.equals(that.aggSchema) : that.aggSchema != null)
            return false;
        if (nameToAlias != null ? !nameToAlias.equals(that.nameToAlias) : that.nameToAlias != null)
            return false;
        if (tagSetting != null ? !tagSetting.equals(that.tagSetting) : that.tagSetting != null)
            return false;
        if (ignoreStrategy != that.ignoreStrategy) return false;
        if (modeName != null ? !modeName.equals(that.modeName) : that.modeName != null)
            return false;
        if (mode != null ? !mode.equals(that.mode) : that.mode != null) return false;
        return fetcher != null ? fetcher.equals(that.fetcher) : that.fetcher == null;
    }
}
