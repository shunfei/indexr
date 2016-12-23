package io.indexr.query.fn;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

import io.indexr.query.expr.Expression;
import io.indexr.query.row.InternalRow;
import io.indexr.query.types.DataType;
import io.indexr.query.types.TypeConverters;

public class One extends Expression {
    public Expression child;

    public One(Expression child) {
        this.child = child;
    }

    @Override
    public DataType dataType() {
        return child.dataType();
    }

    @Override
    public long evalUniformVal(InternalRow input) {
        return TypeConverters.castToUniformVal(1, DataType.IntegerType, child.dataType());
    }

    @Override
    public List<Expression> children() {
        return Collections.singletonList(child);
    }

    @Override
    public Expression withNewChildren(List<Expression> newChildren) {
        assert newChildren.size() == 1;
        return new One(newChildren.get(0));
    }

    @Override
    public List<Object> args() {return Lists.newArrayList(child);}
}
