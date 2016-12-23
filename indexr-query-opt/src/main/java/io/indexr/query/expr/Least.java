package io.indexr.query.expr;

import com.google.common.collect.Lists;

import java.util.List;

import io.indexr.query.row.InternalRow;
import io.indexr.query.types.DataType;

public class Least extends Expression {
    public Expression[] children;
    private DataType dataType;

    public Least(List<Expression> children) {
        this(children.toArray(new Expression[children.size()]));
    }

    public Least(Expression... children) {
        assert children.length > 1;
        this.children = children;
        this.dataType = children[0].dataType();
    }

    @Override
    public boolean foldable() {
        for (Expression e : children) {
            if (!e.foldable()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public DataType dataType() {return dataType;}

    @Override
    public long evalUniformVal(InternalRow input) {
        long leastVal = children[0].evalUniformVal(input);
        long curVal;
        for (int i = 1; i < children.length; i++) {
            curVal = children[i].evalUniformVal(input);
            if (dataType.comparator.compare(leastVal, curVal) > 0) {
                leastVal = curVal;
            }
        }
        return leastVal;
    }

    @Override
    public List<Expression> children() {return Lists.newArrayList(children);}

    @Override
    public Expression withNewChildren(List<Expression> newChildren) {
        return new Least(newChildren);
    }

    @Override
    public List<Object> args() {
        return Lists.newArrayList(children);
    }
}
