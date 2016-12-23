package io.indexr.query.expr.predicate;

import com.google.common.collect.Lists;

import com.carrotsearch.hppc.LongScatterSet;
import com.carrotsearch.hppc.LongSet;

import java.util.Collections;
import java.util.List;

import io.indexr.query.expr.Expression;
import io.indexr.query.expr.UnaryExpression;
import io.indexr.query.row.InternalRow;

public class InSet extends Predicate implements UnaryExpression {
    public Expression child;
    public LongSet hset;

    public InSet(Expression child, LongSet hset) {
        this.child = child;
        this.hset = hset;
        assert !hset.isEmpty();
    }

    public InSet(Expression child, long... list) {
        this(child, LongScatterSet.from(list));
    }

    @Override
    public boolean evalBoolean(InternalRow input) {
        return hset.contains(child.evalUniformVal(input));
    }

    @Override
    public Expression child() {
        return child;
    }

    @Override
    public List<Expression> children() {
        return Collections.singletonList(child);
    }

    @Override
    public Expression withNewChildren(List<Expression> newChildren) {
        assert newChildren.size() == 1;
        return new InSet(newChildren.get(0));
    }

    @Override
    public List<Object> args() {return Lists.newArrayList(child, hset);}
}
