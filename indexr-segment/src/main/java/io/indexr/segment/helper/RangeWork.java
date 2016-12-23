package io.indexr.segment.helper;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class RangeWork {

    protected String segment;
    protected int startPackId;
    protected int endPackId;

    public RangeWork(@JsonProperty("segment") String segment, //
                     @JsonProperty("startPackId") int startPackId,//
                     @JsonProperty("endPackId") int endPackId) {
        this.segment = segment;
        this.startPackId = startPackId;
        this.endPackId = endPackId;
    }

    @JsonProperty("segment")
    public String segment() {
        return segment;
    }

    @JsonProperty("startPackId")
    public int startPackId() {
        return startPackId;
    }

    @JsonProperty("endPackId")
    public int endPackId() {
        return endPackId;
    }

    @Override
    public String toString() {
        return "RangeWork{" +
                "segment='" + segment + '\'' +
                ", startPackId=" + startPackId +
                ", endPackId=" + endPackId +
                '}';
    }

    public static final Comparator<RangeWork> comparator = new Comparator<RangeWork>() {
        @Override
        public int compare(RangeWork o1, RangeWork o2) {
            if (o1 == o2) {
                return 0;
            }
            int res;
            if ((res = o1.segment().compareTo(o2.segment())) != 0) {
                return res;
            }
            return o1.startPackId() - o2.startPackId();
        }
    };

    public static List<RangeWork> compact(List<? extends RangeWork> works) {
        works.sort(comparator);
        List<RangeWork> newList = new ArrayList<>(works.size());
        RangeWork last = null;
        for (RangeWork as : works) {
            if (last == null) {
                last = new RangeWork(as.segment(), as.startPackId(), as.endPackId());
            } else {
                if (StringUtils.equals(as.segment(), last.segment()) && last.endPackId() == as.startPackId()) {
                    last = new RangeWork(last.segment(), last.startPackId(), as.endPackId());
                } else {
                    newList.add(last);
                    last = new RangeWork(as.segment(), as.startPackId(), as.endPackId());
                }
            }
        }
        if (last != null) {
            newList.add(last);
        }
        return newList;
    }


}
