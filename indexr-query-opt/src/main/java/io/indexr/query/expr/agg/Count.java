package io.indexr.query.expr.agg;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import io.indexr.query.expr.Expression;
import io.indexr.query.expr.Literal;
import io.indexr.query.expr.arith.Add;
import io.indexr.query.expr.attr.AttributeReference;
import io.indexr.query.types.DataType;
import io.indexr.util.Lazily;

public class Count extends AggregateFunction {
    public List<Expression> children;

    private Supplier<AttributeReference> count = Lazily.lazily(() -> count = Lazily.value(
            new AttributeReference("count", DataType.LongType)));
    private Supplier<List<AttributeReference>> aggBufferAttributes = Lazily.lazily(() -> aggBufferAttributes = Lazily.value(
            Collections.singletonList(count.get())));
    private Supplier<List<Expression>> initialValues = Lazily.lazily(() -> initialValues = Lazily.value(
            Collections.singletonList(Literal._0L)));
    private Supplier<List<Expression>> updateExpressions = Lazily.lazily(() -> updateExpressions = Lazily.value(
            Collections.singletonList(new Add(count.get(), Literal._1L))));
    private Supplier<List<Expression>> mergeExpressions = Lazily.lazily(() -> mergeExpressions = Lazily.value(
            Collections.singletonList(new Add(bufferLeft(count.get()), bufferRight(count.get())))));

    public Count(List<Expression> children) {
        this.children = children;
    }

    @Override
    public DataType dataType() {return DataType.LongType;}

    @Override
    public List<Expression> children() {return children;}

    @Override
    public Expression withNewChildren(List<Expression> newChildren) {return new Count(newChildren);}

    @Override
    public List<Object> args() {return Lists.newArrayList(children);}

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
        return count.get();
    }
}
