package io.indexr.query.plan.logical;

import com.google.common.collect.Lists;

import java.util.List;

import io.indexr.query.expr.Expression;
import io.indexr.query.expr.attr.Attribute;

public class Limit extends LPUnaryNode {
    public Expression offsetExpr;
    public Expression limitExpr;

    public Limit(Expression offsetExpr, Expression limitExpr, LogicalPlan child) {
        super(child);
        this.offsetExpr = offsetExpr;
        this.limitExpr = limitExpr;
    }

    @Override
    public List<Attribute> output() {
        return child.output();
    }

    @Override
    public LogicalPlan withNewChildren(List<LogicalPlan> newChildren) {
        assert newChildren.size() == 1;
        return new Limit(offsetExpr, limitExpr, newChildren.get(0));
    }

    @Override
    public List<Object> args() {return Lists.newArrayList(offsetExpr, limitExpr, child);}
}
