package io.indexr.query.plan.physical;

import com.google.common.collect.Lists;

import java.util.List;

public abstract class PPBinaryNode extends PhysicalPlan {
    public PhysicalPlan left, right;

    public PPBinaryNode(PhysicalPlan left, PhysicalPlan right) {
        this.left = left;
        this.right = right;
    }

    public PhysicalPlan left() {
        return left;
    }

    public PhysicalPlan right() {
        return right;
    }

    @Override
    public List<PhysicalPlan> children() {return Lists.newArrayList(left(), right());}
}