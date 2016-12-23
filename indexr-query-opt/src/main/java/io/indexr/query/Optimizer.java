package io.indexr.query;

import com.google.common.collect.Lists;

import java.util.List;

import io.indexr.query.expr.Cast;
import io.indexr.query.plan.logical.LogicalPlan;

public class Optimizer extends RuleExecutor<LogicalPlan> {

    private Strategy fixedPoint = new Strategy(100);

    @Override
    protected List<Batch> batches() {
        return Lists.newArrayList(
                new Batch("Operator Optimizations", fixedPoint,
                        new SimplifyCasts())
        );
    }

    /**
     * Removes [[Cast Casts]] that are unnecessary because the input is already the correct type.
     */
    private class SimplifyCasts implements Rule<LogicalPlan> {
        @Override
        public LogicalPlan apply(LogicalPlan plan) {
            return plan.transformAllExpressions(e -> {
                if (e instanceof Cast) {
                    Cast cast = (Cast) e;
                    if (cast.dataType() == cast.child().dataType()) {
                        return cast.child();
                    }
                }
                return e;
            });
        }
    }
}
