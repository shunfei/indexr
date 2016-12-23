package io.indexr.query;

import io.indexr.query.plan.logical.LogicalPlan;

public interface Catalog {
    default boolean tableExists(String name) {return true;}

    LogicalPlan lookupRelation(String name);
}
