package io.indexr.query.expr.predicate;

import com.google.common.collect.Lists;

import java.util.List;

import io.indexr.query.expr.Expression;
import io.indexr.query.row.InternalRow;
import io.indexr.query.types.DataType;

public class NotEqual extends BinaryPredicate {
    public NotEqual(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    public boolean evalBoolean(InternalRow input) {
        if (left.dataType() == DataType.StringType) {
            return !left.evalString(input).equals(right.evalString(input));
        } else {
            return left.evalUniformVal(input) != right.evalUniformVal(input);
        }
    }

    @Override
    public Expression withNewChildren(List<Expression> newChildren) {
        assert newChildren.size() == 2;
        return new EqualTo(newChildren.get(0), newChildren.get(1));
    }

    @Override
    public List<Object> args() {return Lists.newArrayList(left, right);}
}
