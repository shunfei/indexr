package io.indexr.segment.rt;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;

import io.indexr.util.JsonUtil;

public class AggSchema implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("grouping")
    public final boolean grouping;
    @JsonProperty("dims")
    public final List<String> dims;
    @JsonProperty("metrics")
    public final List<Metric> metrics;

    public AggSchema(@JsonProperty("grouping") boolean grouping,
                     @JsonProperty("dims") List<String> dims,
                     @JsonProperty("metrics") List<Metric> metrics) {
        this.grouping = grouping;
        this.dims = dims;
        this.metrics = metrics;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AggSchema aggSchema = (AggSchema) o;

        if (grouping != aggSchema.grouping) return false;
        if (dims != null ? !dims.equals(aggSchema.dims) : aggSchema.dims != null) return false;
        return metrics != null ? metrics.equals(aggSchema.metrics) : aggSchema.metrics == null;
    }

    @Override
    public String toString() {
        return JsonUtil.toJson(this);
    }
}
