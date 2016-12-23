package io.indexr.query.plan.logical;

import java.util.Collections;
import java.util.List;

import io.indexr.query.expr.attr.Attribute;

public class OneRowRelation extends LPLeafNode {

    @Override
    public List<Attribute> output() {
        return Collections.emptyList();
    }

    //@Override
    //public List<Expression> expressions() {
    //    return Collections.emptyList();
    //}

    @Override
    public LogicalPlan withNewChildren(List<LogicalPlan> newChildren) {
        return this;
    }

    @Override
    public List<Object> args() {return Collections.emptyList();}
}
