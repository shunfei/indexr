package io.indexr.query.plan.logical;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import io.indexr.query.expr.attr.Attribute;

public abstract class LPLeafNode extends LogicalPlan {
    @Override
    public List<LogicalPlan> children() {return Collections.emptyList();}

    @Override
    public Set<Attribute> producedAttributes() {return this.outputSet();}
}