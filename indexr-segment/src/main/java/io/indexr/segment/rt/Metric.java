package io.indexr.segment.rt;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.indexr.util.Strings;

public class Metric {
    @JsonProperty("name")
    public final String name;
    @JsonIgnore
    public final int aggType;

    @JsonCreator
    public Metric(@JsonProperty("name") String name,
                  @JsonProperty("agg") String aggType) {
        assert name != null;
        assert aggType != null;

        this.name = name.toLowerCase().trim();
        this.aggType = AggType.forName(aggType);
    }

    @JsonProperty("agg")
    public String aggName() {
        return AggType.getName(aggType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Metric metric = (Metric) o;
        return Strings.equals(name, metric.name) && aggType == metric.aggType;
    }
}
