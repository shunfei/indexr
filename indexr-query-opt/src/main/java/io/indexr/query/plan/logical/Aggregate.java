package io.indexr.query.plan.logical;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.stream.Collectors;

import io.indexr.query.expr.Expression;
import io.indexr.query.expr.attr.Attribute;
import io.indexr.query.expr.attr.NamedExpression;

public class Aggregate extends LPUnaryNode {
    public List<Expression> groupingExpressions;
    public List<NamedExpression> aggregateExpressions;

    public Aggregate(List<Expression> groupingExpressions, List<NamedExpression> aggregateExpressions, LogicalPlan child) {
        super(child);
        this.groupingExpressions = groupingExpressions;
        this.aggregateExpressions = aggregateExpressions;
    }

    @Override
    public List<Attribute> output() {
        return aggregateExpressions.stream().map(NamedExpression::toAttribute).collect(Collectors.toList());
    }

    //@Override
    //public List<Expression> expressions() {
    //    return new ArrayList<>(aggregateExpressions);
    //}

    @Override
    public LogicalPlan withNewChildren(List<LogicalPlan> newChildren) {
        assert newChildren.size() == 1;
        return new Aggregate(groupingExpressions, aggregateExpressions, newChildren.get(0));
    }

    @Override
    public List<Object> args() {return Lists.newArrayList(groupingExpressions, aggregateExpressions, child);}
}
