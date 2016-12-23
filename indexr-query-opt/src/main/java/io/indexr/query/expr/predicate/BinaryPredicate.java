package io.indexr.query.expr.predicate;

import com.google.common.collect.Lists;

import java.util.List;

import io.indexr.query.expr.BinaryExpression;
import io.indexr.query.expr.Expression;

public abstract class BinaryPredicate extends Predicate implements BinaryExpression {
    public Expression left, right;

    public BinaryPredicate(Expression left, Expression right) {
        //assert left.dataType() == right.dataType();
        this.left = left;
        this.right = right;
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
    public List<Expression> children() {
        return Lists.newArrayList(left, right);
    }

    //@Override
    //public Expression withNewChildren(List<Expression> newChildren) {
    //    assert newChildren.size() == 2;
    //    left = newChildren.get(0);
    //    right = newChildren.get(1);
    //    return this;
    //}
}
