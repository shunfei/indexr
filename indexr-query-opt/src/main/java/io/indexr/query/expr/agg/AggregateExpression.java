package io.indexr.query.expr.agg;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.indexr.query.expr.Expression;
import io.indexr.query.expr.attr.Attribute;
import io.indexr.query.row.InternalRow;
import io.indexr.query.types.DataType;

public class AggregateExpression extends Expression {
    public AggregateFunction aggregateFunction;
    public AggregateMode mode;

    public AggregateExpression(AggregateFunction aggregateFunction, AggregateMode mode) {
        this.aggregateFunction = aggregateFunction;
        this.mode = mode;
    }

    @Override
    public DataType dataType() {return aggregateFunction.dataType();}

    @Override
    public long evalUniformVal(InternalRow input) {
        throw new UnsupportedOperationException(String.format("Cannot evaluate expression: %s", this));
    }

    @Override
    public Set<Attribute> references() {
        switch (mode) {
            case Partial:
            case Complete:
                return aggregateFunction.references();
            case PartialMerge:
            case Final:
                return new HashSet<>(aggregateFunction.aggBufferAttributes());
            default:
                return Collections.emptySet();
        }
    }

    @Override
    public List<Expression> children() {
        return Collections.singletonList(aggregateFunction);
    }

    @Override
    public Expression withNewChildren(List<Expression> newChildren) {
        assert newChildren.size() == 1;
        return new AggregateExpression((AggregateFunction) newChildren.get(0), mode);
    }

    @Override
    public List<Object> args() {
        return Lists.newArrayList(aggregateFunction, mode);
    }

    @Override
    public String prettyString() {
        return aggregateFunction.prettyString();
    }

    @Override
    public String toString() {
        return String.format("aggExpr(%s,mode=%s)", aggregateFunction, mode);
    }

}
