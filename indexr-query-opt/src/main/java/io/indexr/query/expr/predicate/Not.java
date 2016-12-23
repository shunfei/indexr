package io.indexr.query.expr.predicate;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

import io.indexr.query.expr.Expression;
import io.indexr.query.expr.UnaryExpression;
import io.indexr.query.row.InternalRow;

public class Not extends Predicate implements UnaryExpression {
    public Expression child;

    public Not(Expression child) {
        this.child = child;
    }

    @Override
    public boolean evalBoolean(InternalRow input) {
        return !child.evalBoolean(input);
    }

    @Override
    public List<Expression> children() {
        return Collections.singletonList(child);
    }

    @Override
    public Expression child() {
        return child;
    }

    @Override
    public Expression withNewChildren(List<Expression> newChildren) {
        assert newChildren.size() == 1;
        return new Not(newChildren.get(0));
    }

    @Override
    public List<Object> args() {return Lists.newArrayList(child);}
}
