package io.indexr.query.plan.logical;

import com.google.common.collect.Lists;

import java.util.List;

import io.indexr.query.expr.attr.Attribute;

public class NoOp extends LPUnaryNode {
    public NoOp(LogicalPlan child) {
        super(child);
    }

    @Override
    public List<Attribute> output() {
        return child.output();
    }

    @Override
    public LogicalPlan withNewChildren(List<LogicalPlan> newChildren) {
        assert newChildren.size() == 1;
        return new NoOp(newChildren.get(0));
    }

    @Override
    public List<Object> args() {return Lists.newArrayList(child);}
}
