package io.indexr.query.expr.arith;

import com.google.common.collect.Lists;

import java.util.List;

import io.indexr.query.expr.Expression;
import io.indexr.query.row.InternalRow;

public class BitwiseAnd extends BinaryArithmetic {

    public BitwiseAnd(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    public long evalUniformVal(InternalRow input) {
        return left.evalUniformVal(input) & right.evalUniformVal(input);
    }

    @Override
    public Expression withNewChildren(List<Expression> newChildren) {
        assert newChildren.size() == 2;
        return new BitwiseAnd(newChildren.get(0), newChildren.get(1));
    }

    @Override
    public List<Object> args() {return Lists.newArrayList(left, right);}
}
