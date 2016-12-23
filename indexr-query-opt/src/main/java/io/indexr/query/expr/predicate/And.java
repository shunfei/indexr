package io.indexr.query.expr.predicate;

import com.google.common.collect.Lists;

import java.util.List;

import io.indexr.query.expr.Expression;
import io.indexr.query.row.InternalRow;

public class And extends BinaryPredicate {
    public And(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    public boolean evalBoolean(InternalRow input) {
        return left.evalBoolean(input) && right.evalBoolean(input);
    }

    @Override
    public Expression withNewChildren(List<Expression> newChildren) {
        assert newChildren.size() == 2;
        return new And(newChildren.get(0), newChildren.get(1));
    }

    @Override
    public List<Object> args() {return Lists.newArrayList(left, right);}
}
