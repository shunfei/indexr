package io.indexr.query.plan.logical;

import com.google.common.collect.Lists;

import java.util.List;

import io.indexr.query.expr.Expression;
import io.indexr.query.expr.attr.Attribute;

public class Filter extends LPUnaryNode {
    public Expression condition;

    public Filter(Expression condition, LogicalPlan child) {
        super(child);
        this.condition = condition;
    }

    @Override
    public List<Attribute> output() {
        return child.output();
    }

    @Override
    public LogicalPlan withNewChildren(List<LogicalPlan> newChildren) {
        assert newChildren.size() == 1;
        return new Filter(condition, newChildren.get(0));
    }

    @Override
    public List<Object> args() {return Lists.newArrayList(condition, child);}
}
