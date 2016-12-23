package io.indexr.query.expr.arith;

import com.google.common.collect.Lists;

import java.util.List;

import io.indexr.query.expr.BinaryExpression;
import io.indexr.query.expr.Expression;
import io.indexr.query.types.DataType;

public abstract class BinaryArithmetic extends Expression implements BinaryExpression {
    public Expression left, right;
    protected DataType dataType;
    protected DataType leftType;
    protected DataType rightType;

    public BinaryArithmetic(Expression left, Expression right) {
        this.left = left;
        this.right = right;
    }

    protected DataType defaultType() {
        return null;
    }

    protected void initType() {
        if (dataType == null) {
            leftType = left.dataType();
            rightType = right.dataType();
            DataType defaultType = defaultType();
            if (defaultType == null) {
                dataType = BinaryArithmetic.calType(left.dataType(), right.dataType());
            } else {
                dataType = defaultType;
            }
        }
    }

    @Override
    public Expression left() {
        return left;
    }

    @Override
    public Expression right() {
        return right;
    }

    @Override
    public DataType dataType() {
        initType();
        return dataType;
    }

    @Override
    public List<Expression> children() {
        return Lists.newArrayList(left, right);
    }

    @Override
    public boolean resolved() {
        return childrenResolved() && checkInputDataTypes().isSuccess;
    }

    public static DataType calType(DataType type1, DataType type2) {
        return type1.ordinal() >= type2.ordinal() ? type1 : type2;
    }
}
