package io.indexr.query;

import java.util.Iterator;

import io.indexr.query.plan.logical.LogicalPlan;
import io.indexr.query.plan.physical.PhysicalPlan;
import io.indexr.query.row.InternalRow;

public class QueryExecution {
    private QueryContext queryContext;
    private LogicalPlan logicalPlan;

    public QueryExecution(QueryContext queryContext, LogicalPlan logicalPlan) {
        this.queryContext = queryContext;
        this.logicalPlan = logicalPlan;
    }

    private LogicalPlan analyzedPlan;
    private LogicalPlan optimizedPlan;
    private PhysicalPlan physicalPlan;
    private Iterator<InternalRow> result;

    public LogicalPlan logicalPlan() {
        return logicalPlan;
    }

    public LogicalPlan analyzedPlan() {
        if (analyzedPlan == null) {
            analyzedPlan = queryContext.analyzer().execute(logicalPlan);
            new CheckAnalysis().checkAnalysis(analyzedPlan);
        }
        return analyzedPlan;
    }

    public LogicalPlan optimizedPlan() {
        if (optimizedPlan == null) {
            optimizedPlan = queryContext.optimizer().execute(analyzedPlan());
        }
        return optimizedPlan;
    }

    public PhysicalPlan physicalPlan() {
        if (physicalPlan == null) {
            physicalPlan = queryContext.planner().plan(optimizedPlan()).next();
        }
        return physicalPlan;
    }

    public Iterator<InternalRow> result() {
        if (result == null) {
            result = physicalPlan().execute();
        }
        return result;
    }
}
