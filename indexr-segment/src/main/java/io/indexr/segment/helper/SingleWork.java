package io.indexr.segment.helper;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SingleWork extends RangeWork {
    public SingleWork(@JsonProperty("segment") String segment,
                      @JsonProperty("packId") int packId) {
        super(segment, packId, packId + 1);
    }

    @JsonProperty("segment")
    public String segment() {
        return segment;
    }

    @JsonProperty("packId")
    public int packId() {
        return startPackId;
    }

    @Override
    public String toString() {
        return "SingleWork{" +
                "segment='" + segment + '\'' +
                ", packId=" + startPackId +
                '}';
    }
}
