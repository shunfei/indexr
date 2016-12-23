package io.indexr.query.plan.logical;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

import io.indexr.query.expr.attr.Attribute;

public class UnresolvedRelation extends LPLeafNode {
    public String tableName;

    public UnresolvedRelation(String tableName) {
        this.tableName = tableName;
    }

    public String tableName() {
        return tableName;
    }

    @Override
    public List<Attribute> output() {return Collections.emptyList();}

    @Override
    public LogicalPlan withNewChildren(List<LogicalPlan> newChildren) {return this;}

    @Override
    public List<Object> args() {return Lists.newArrayList(tableName);}
}
