package io.indexr.segment.query;

import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;

import io.indexr.query.expr.attr.Attribute;
import io.indexr.query.plan.logical.LPLeafNode;
import io.indexr.query.plan.logical.LogicalPlan;
import io.indexr.segment.Segment;

public class SegmentRelation extends LPLeafNode {
    public final List<Attribute> output;
    public final List<Segment> segments;

    public SegmentRelation(List<Attribute> output, List<Segment> segments) {
        this.output = output;
        this.segments = segments;
    }

    public SegmentRelation(List<Attribute> output, Segment... segments) {
        this(output, Arrays.asList(segments));
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    public LogicalPlan withNewChildren(List<LogicalPlan> newChildren) {
        assert newChildren.size() == 0;
        return new SegmentRelation(output, segments);
    }

    @Override
    public List<Object> args() {return Lists.newArrayList(output);}
}