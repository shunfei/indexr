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

public class Sum extends AggregateFunction {
    public Expression child;

    private Supplier<AttributeReference> sum = Lazily.lazily(() -> sum = Lazily.value(
            new AttributeReference("sum", child.dataType())));
    private Supplier<List<AttributeReference>> aggBufferAttributes = Lazily.lazily(() -> aggBufferAttributes = Lazily.value(
            Collections.singletonList(sum.get())));
    private Supplier<List<Expression>> initialValues = Lazily.lazily(() -> initialValues = Lazily.value(
            Collections.singletonList(new Literal(0, child.dataType()))));
    private Supplier<List<Expression>> updateExpressions = Lazily.lazily(() -> updateExpressions = Lazily.value(
            Collections.singletonList(new Add(sum.get(), child))));
    private Supplier<List<Expression>> mergeExpressions = Lazily.lazily(() -> mergeExpressions = Lazily.value(
            Collections.singletonList(new Add(bufferLeft(sum.get()), bufferRight(sum.get())))));

    public Sum(Expression child) {
        this.child = child;
    }

    @Override
    public DataType dataType() {return child.dataType();}

    @Override
    public List<Expression> children() {return Collections.singletonList(child);}

    @Override
    public Expression withNewChildren(List<Expression> newChildren) {
        assert newChildren.size() == 1;
        return new Sum(newChildren.get(0));
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
        return sum.get();
    }
}
