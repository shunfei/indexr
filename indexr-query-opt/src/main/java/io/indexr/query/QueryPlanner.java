package io.indexr.query;

import java.util.Iterator;
import java.util.List;

import io.indexr.query.plan.logical.LogicalPlan;

import static io.indexr.util.Trick.concatToList;
import static io.indexr.util.Trick.flatMapToList;

public abstract class QueryPlanner<PhysicalPlan extends TreeNode<PhysicalPlan>> {
    /** A list of execution strategies that can be used by the planner */
    protected abstract List<Strategy<PhysicalPlan>> strategies();

    /**
     * Returns a placeholder for a physical plan that executes `plan`. This placeholder will be
     * filled in automatically by the QueryPlanner using the other execution strategies that are
     * available.
     */
    protected PhysicalPlan planLater(LogicalPlan plan) {
        return this.plan(plan).next();
    }

    protected Iterator<PhysicalPlan> plan(LogicalPlan plan) {
        // Obviously a lot to do here still...
        Iterator<PhysicalPlan> iter = flatMapToList(concatToList(strategies()), s -> s.apply(plan), /*ignoreNull*/true).iterator();
        assert iter.hasNext() : String.format("No plan for %s", plan);
        return iter;
    }
}