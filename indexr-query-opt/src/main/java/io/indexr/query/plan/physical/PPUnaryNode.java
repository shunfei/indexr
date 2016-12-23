package io.indexr.query.plan.physical;

import java.util.Collections;
import java.util.List;

public abstract class PPUnaryNode extends PhysicalPlan {
    public PhysicalPlan child;

    public PPUnaryNode(PhysicalPlan child) {
        this.child = child;
    }

    public PhysicalPlan child() {
        return child;
    }

    @Override
    public List<PhysicalPlan> children() {return Collections.singletonList(child);}
}
