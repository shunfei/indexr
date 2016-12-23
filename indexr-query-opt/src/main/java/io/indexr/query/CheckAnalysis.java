package io.indexr.query;

import io.indexr.query.plan.logical.LogicalPlan;

public class CheckAnalysis {
    public void checkAnalysis(LogicalPlan plan) {
        if (!plan.resolved()) {
            throw new UnresolvedException(String.format("%s", plan));
        }
    }
}
