package io.indexr.query.plan.logical;

import java.util.Collections;
import java.util.List;

public abstract class LPUnaryNode extends LogicalPlan {
    public LogicalPlan child;

    public LPUnaryNode(LogicalPlan child) {
        this.child = child;
    }

    public LogicalPlan child() {return child;}

    @Override
    public List<LogicalPlan> children() {return Collections.singletonList(child);}
}