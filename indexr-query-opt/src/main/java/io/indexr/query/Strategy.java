package io.indexr.query;

import java.util.List;

import io.indexr.query.plan.logical.LogicalPlan;

public interface Strategy<PhysicalPlan extends TreeNode<PhysicalPlan>> {
    List<PhysicalPlan> apply(LogicalPlan plan);
}
