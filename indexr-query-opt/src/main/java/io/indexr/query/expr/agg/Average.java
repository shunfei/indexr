package io.indexr.query.expr.agg;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import io.indexr.query.expr.Cast;
import io.indexr.query.expr.Expression;
import io.indexr.query.expr.Literal;
import io.indexr.query.expr.arith.Add;
import io.indexr.query.expr.arith.Divide;
import io.indexr.query.expr.attr.AttributeReference;
import io.indexr.query.types.DataType;
import io.indexr.util.Lazily;

public class Average extends AggregateFunction {
    public Expression child;

    private Supplier<AttributeReference> sum = Lazily.lazily(() -> sum = Lazily.value(
            new AttributeReference("sum", DataType.DoubleType)));
    private Supplier<AttributeReference> count = Lazily.lazily(() -> count = Lazily.value(
            new AttributeReference("count", DataType.LongType)));
    private Supplier<List<AttributeReference>> aggBufferAttributes = Lazily.lazily(() -> aggBufferAttributes = Lazily.value(
            Lists.newArrayList(sum.get(), count.get())));
    private Supplier<List<Expression>> initialValues = Lazily.lazily(() -> initialValues = Lazily.value(
            Lists.newArrayList(new Literal(0, DataType.DoubleType), new Literal(0, DataType.LongType))));
    private Supplier<List<Expression>> updateExpressions = Lazily.lazily(() -> updateExpressions = Lazily.value(
            Lists.newArrayList(new Add(sum.get(), Cast.tryCast(child, DataType.DoubleType)), new Add(count.get(), Literal._1L))));
    private Supplier<List<Expression>> mergeExpressions = Lazily.lazily(() -> mergeExpressions = Lazily.value(
            Lists.newArrayList(new Add(bufferLeft(sum.get()), bufferRight(sum.get())), new Add(bufferLeft(count.get()), bufferRight(count.get())))));
    private Supplier<Expression> evaluateExpression = Lazily.lazily(() -> evaluateExpression = Lazily.value(
            new Divide(sum.get(), Cast.tryCast(count.get(), DataType.DoubleType))));

    public Average(Expression child) {
        this.child = child;
    }

    @Override
    public DataType dataType() {
        return DataType.DoubleType;
    }

    @Override
    public List<Expression> children() {
        return Collections.singletonList(child);
    }

    @Override
    public Expression withNewChildren(List<Expression> newChildren) {
        assert newChildren.size() == 1;
        return new Average(newChildren.get(0));
    }

    @Override
    public List<Object> args() {
        return Lists.newArrayList(child);
    }

    @Override
    public List<AttributeReference> aggBufferAttributes() {
        return aggBufferAttributes.get();
    }

    @Override
    public List<Expression> initialValues() {
        return initialValues.get();
    }

    @Override
    public List<Expression> updateExpressions() {
        return updateExpressions.get();
    }

    @Override
    public List<Expression> mergeExpressions() {
        return mergeExpressions.get();
    }

    @Override
    public Expression evaluateExpression() {
        return evaluateExpression.get();
    }
}
