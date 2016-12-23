package io.indexr.query.plan.physical;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import io.indexr.query.expr.attr.Attribute;

public abstract class PPLeafNode extends PhysicalPlan {
    @Override
    public List<PhysicalPlan> children() {return Collections.emptyList();}

    @Override
    public Set<Attribute> producedAttributes() {return outputSet();}
}
