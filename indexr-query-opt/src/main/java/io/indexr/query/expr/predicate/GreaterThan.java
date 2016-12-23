package io.indexr.query.expr.predicate;

import com.google.common.collect.Lists;

import java.util.List;

import io.indexr.query.expr.Expression;
import io.indexr.query.row.InternalRow;

import static io.indexr.query.types.TypeConverters.castToDouble;
import static io.indexr.query.types.TypeConverters.castToFloat;
import static io.indexr.query.types.TypeConverters.castToInt;
import static io.indexr.query.types.TypeConverters.castToLong;

public class GreaterThan extends BinaryPredicate {
    public GreaterThan(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    public boolean evalBoolean(InternalRow input) {
        //assert left.dataType() == right.dataType();
        switch (left.dataType()) {
            case BooleanType:
                return left.evalUniformVal(input) > right.evalUniformVal(input);
            case IntegerType:
                return left.evalInt(input) > castToInt(right.evalUniformVal(input), right.dataType());
            case LongType:
                return left.evalLong(input) > castToLong(right.evalUniformVal(input), right.dataType());
            case FloatType:
                return left.evalFloat(input) > castToFloat(right.evalUniformVal(input), right.dataType());
            case DoubleType:
                return left.evalDouble(input) > castToDouble(right.evalUniformVal(input), right.dataType());
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public Expression withNewChildren(List<Expression> newChildren) {
        assert newChildren.size() == 2;
        return new GreaterThan(newChildren.get(0), newChildren.get(1));
    }

    @Override
    public List<Object> args() {return Lists.newArrayList(left, right);}
}
