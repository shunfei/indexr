package io.indexr.query.plan.logical;

import com.google.common.collect.Lists;

import java.util.List;

public abstract class LPBinaryNode extends LogicalPlan {
    public LogicalPlan left, right;

    public LPBinaryNode(LogicalPlan left, LogicalPlan right) {
        this.left = left;
        this.right = right;
    }

    public LogicalPlan left() {return left;}

    public LogicalPlan right() {return right;}

    @Override
    public List<LogicalPlan> children() {return Lists.newArrayList(left, right);}
}