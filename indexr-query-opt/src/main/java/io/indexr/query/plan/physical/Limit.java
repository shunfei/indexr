package io.indexr.query.plan.physical;

import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import io.indexr.query.expr.attr.Attribute;
import io.indexr.query.row.InternalRow;

public class Limit extends PPUnaryNode {
    public long offset;
    public long limit;

    public Limit(long offset, long limit, PhysicalPlan child) {
        super(child);
        this.offset = offset;
        this.limit = limit;
    }

    @Override
    protected Iterator<InternalRow> doExecute() {
        Iterator<InternalRow> childResItr = child.execute();
        Stream<InternalRow> sortedStream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(childResItr, 0), false);
        return sortedStream.skip(offset).limit(limit).iterator();
    }

    @Override
    public List<Attribute> output() {
        return child.output();
    }

    @Override
    public PhysicalPlan withNewChildren(List<PhysicalPlan> newChildren) {
        assert newChildren.size() == 1;
        return new Limit(offset, limit, newChildren.get(0));
    }

    @Override
    public List<Object> args() {return Lists.newArrayList(offset, limit, child);}
}
