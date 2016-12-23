package io.indexr.query.plan.logical;

import com.google.common.collect.Lists;

import java.util.List;

import io.indexr.query.expr.SortOrder;
import io.indexr.query.expr.attr.Attribute;

public class Sort extends LPUnaryNode {
    public List<SortOrder> order;

    public Sort(List<SortOrder> order, LogicalPlan child) {
        super(child);
        this.order = order;
    }

    @Override
    public List<Attribute> output() {
        return child.output();
    }

    //@Override
    //public List<Expression> expressions() {
    //    return Collections.emptyList();
    //}

    @Override
    public LogicalPlan withNewChildren(List<LogicalPlan> newChildren) {
        assert newChildren.size() == 1;
        return new Sort(order, newChildren.get(0));
    }

    @Override
    public List<Object> args() {return Lists.newArrayList(order, child);}
}
