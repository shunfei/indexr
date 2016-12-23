package io.indexr.query.expr.agg;

import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.function.Supplier;

import io.indexr.query.expr.Expression;
import io.indexr.query.expr.attr.AttributeReference;
import io.indexr.query.row.InternalRow;
import io.indexr.util.Lazily;
import io.indexr.util.Trick;

public abstract class AggregateFunction extends Expression {
    private Supplier<List<AttributeReference>> inputAggBufferAttributes = Lazily.lazily(() -> inputAggBufferAttributes = Lazily.value(
            Trick.mapToList(aggBufferAttributes(), AttributeReference::newInstance)
    ));

    public abstract List<AttributeReference> aggBufferAttributes();

    public StructType aggBufferSchema() {
        return StructType.fromAttributes(aggBufferAttributes());
    }

    public List<AttributeReference> inputAggBufferAttributes() {
        return inputAggBufferAttributes.get();
    }

    public abstract List<Expression> initialValues();

    public abstract List<Expression> updateExpressions();

    public abstract List<Expression> mergeExpressions();

    public abstract Expression evaluateExpression();

    protected AttributeReference bufferLeft(AttributeReference a) {
        return a;
    }

    protected AttributeReference bufferRight(AttributeReference a) {
        return inputAggBufferAttributes().get(aggBufferAttributes().indexOf(a));
    }

    @Override
    public long evalUniformVal(InternalRow input) {
        throw new UnsupportedOperationException(String.format("Cannot evaluate expression: %s", this));
    }

    /**
     * Wraps this [[AggregateFunction]] in an [[AggregateExpression]] and set isDistinct
     * field of the [[AggregateExpression]] to the given value because
     * [[AggregateExpression]] is the container of an [[AggregateFunction]], aggregation mode,
     * and the flag indicating if this aggregation is distinct aggregation or not.
     * An [[AggregateFunction]] should not be used without being wrapped in
     * an [[AggregateExpression]].
     */
    public AggregateExpression toAggregateExpression() {
        return new AggregateExpression(this, AggregateMode.Complete);
    }
}
