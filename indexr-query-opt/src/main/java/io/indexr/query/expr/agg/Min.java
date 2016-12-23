package io.indexr.query.expr.agg;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import io.indexr.query.expr.Expression;
import io.indexr.query.expr.Least;
import io.indexr.query.expr.Literal;
import io.indexr.query.expr.attr.AttributeReference;
import io.indexr.query.types.DataType;
import io.indexr.util.Lazily;

public class Min extends AggregateFunction {
    public Expression child;

    private Supplier<AttributeReference> min = Lazily.lazily(() -> min = Lazily.value(
            new AttributeReference("min", child.dataType())));
    private Supplier<List<AttributeReference>> aggBufferAttributes = Lazily.lazily(() -> aggBufferAttributes = Lazily.value(
            Collections.singletonList(min.get())));
    private Supplier<List<Expression>> initialValues = Lazily.lazily(() -> initialValues = Lazily.value(Collections.singletonList(
            new Literal(child.dataType().max, child.dataType()))));
    private Supplier<List<Expression>> updateExpressions = Lazily.lazily(() -> updateExpressions = Lazily.value(
            Collections.singletonList(new Least(min.get(), child))));
    private Supplier<List<Expression>> mergeExpressions = Lazily.lazily(() -> mergeExpressions = Lazily.value(
            Collections.singletonList(new Least(bufferLeft(min.get()), bufferRight(min.get())))));

    public Min(Expression child) {
        this.child = child;
    }

    @Override
    public DataType dataType() {return child.dataType();}

    @Override
    public List<Expression> children() {return Collections.singletonList(child);}

    @Override
    public Expression withNewChildren(List<Expression> newChildren) {
        assert newChildren.size() == 1;
        return new Min(newChildren.get(0));
    }

    @Override
    public List<Object> args() {return Lists.newArrayList(child);}

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
        return min.get();
    }
}
