package io.indexr.query.expr;

import com.google.common.collect.Lists;

import org.apache.spark.unsafe.types.UTF8String;

import java.util.Collections;
import java.util.List;

import io.indexr.query.row.InternalRow;
import io.indexr.query.types.DataType;
import io.indexr.query.types.TypeConverters;

public class Cast extends Expression implements UnaryExpression {
    public Expression child;
    public DataType dataType;
    private DataType childDataType;

    public Cast(Expression child, DataType dataType) {
        this.child = child;
        this.dataType = dataType;
    }

    @Override
    public List<Object> args() {
        return Lists.newArrayList(child, dataType);
    }

    @Override
    public Expression withNewArgs(List<Object> args) {
        return new Cast((Expression) args.get(0), (DataType) args.get(1));
    }

    @Override
    public DataType dataType() {return dataType;}

    @Override
    public List<Expression> children() {return Collections.singletonList(child);}

    @Override
    public Expression withNewChildren(List<Expression> newChildren) {
        assert newChildren.size() == 1;
        return new Cast(newChildren.get(0), dataType);
    }

    @Override
    public Expression child() {return child;}

    @Override
    public long evalUniformVal(InternalRow input) {
        if (childDataType == null) {
            childDataType = child.dataType();
        }
        if (dataType == childDataType) {
            return child.evalUniformVal(input);
        }
        if (childDataType == DataType.StringType) {
            return TypeConverters.castStringToUniformVal(child.evalString(input).toString(), dataType);
        } else {
            return TypeConverters.castToUniformVal(child.evalUniformVal(input), childDataType, dataType);
        }
    }

    @Override
    public UTF8String evalString(InternalRow input) {
        if (childDataType == null) {
            childDataType = child.dataType();
        }
        if (dataType == childDataType) {
            return child.evalString(input);
        }
        return UTF8String.fromString(TypeConverters.castToString(child.evalUniformVal(input), childDataType));
    }

    public static Expression tryCast(Expression e, DataType dataType) {
        return dataType == e.dataType() ? e : new Cast(e, dataType);
    }
}
