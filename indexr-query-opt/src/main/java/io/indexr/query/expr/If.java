package io.indexr.query.expr;

import com.google.common.collect.Lists;

import org.apache.spark.unsafe.types.UTF8String;

import java.util.List;

import io.indexr.query.row.InternalRow;
import io.indexr.query.types.DataType;

public class If extends Expression {
    private Expression condition;
    private Expression expr1, expr2;

    public If(Expression condition, Expression expr1, Expression expr2) {
        this.condition = condition;
        this.expr1 = expr1;
        this.expr2 = expr2;
    }

    @Override
    public DataType dataType() {
        return expr1.dataType();
    }

    @Override
    public long evalUniformVal(InternalRow input) {
        return condition.evalBoolean(input) ? expr1.evalUniformVal(input) : expr2.evalUniformVal(input);
    }

    @Override
    public UTF8String evalString(InternalRow input) {
        return condition.evalBoolean(input) ? expr1.evalString(input) : expr2.evalString(input);
    }

    @Override
    public List<Expression> children() {
        return Lists.newArrayList(condition, expr1, expr2);
    }

    @Override
    public Expression withNewChildren(List<Expression> newChildren) {
        return new If((Expression) newChildren.get(0), (Expression) newChildren.get(1), (Expression) newChildren.get(2));
    }

    @Override
    public List<Object> args() {
        return Lists.newArrayList(condition, expr1, expr2);
    }
}
