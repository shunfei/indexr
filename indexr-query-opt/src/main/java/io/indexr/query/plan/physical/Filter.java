package io.indexr.query.plan.physical;

import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

import io.indexr.query.expr.Expression;
import io.indexr.query.expr.attr.Attribute;
import io.indexr.query.row.InternalRow;
import io.indexr.util.FilterIterator;

public class Filter extends PPUnaryNode {
    public Expression condition;

    public Filter(Expression condition, PhysicalPlan child) {
        super(child);
        this.condition = condition;
    }

    @Override
    protected Iterator<InternalRow> doExecute() {
        Iterator<InternalRow> childResItr = child.execute();
        Predicate<InternalRow> filter = newPredicate(condition, child.output());
        return new FilterIterator<>(childResItr, filter);
    }

    @Override
    public List<Attribute> output() {
        return child.output();
    }

    @Override
    public PhysicalPlan withNewChildren(List<PhysicalPlan> newChildren) {
        assert newChildren.size() == 1;
        return new Filter(condition, newChildren.get(0));
    }

    @Override
    public List<Object> args() {return Lists.newArrayList(condition, child);}
}
