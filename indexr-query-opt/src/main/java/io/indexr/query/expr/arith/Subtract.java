package io.indexr.query.expr.arith;

import com.google.common.collect.Lists;

import java.util.List;

import io.indexr.query.expr.Expression;
import io.indexr.query.row.InternalRow;

import static io.indexr.query.types.TypeConverters.castToDouble;
import static io.indexr.query.types.TypeConverters.castToFloat;
import static io.indexr.query.types.TypeConverters.castToInt;
import static io.indexr.query.types.TypeConverters.castToLong;

public class Subtract extends BinaryArithmetic {
    public Subtract(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    public long evalUniformVal(InternalRow input) {
        initType();
        switch (dataType) {
            case IntegerType:
                return castToInt(left.evalUniformVal(input), leftType) - castToInt(right.evalUniformVal(input), rightType);
            case LongType:
                return castToLong(left.evalUniformVal(input), leftType) - castToLong(right.evalUniformVal(input), rightType);
            case FloatType:
                return Double.doubleToRawLongBits(castToFloat(left.evalUniformVal(input), leftType) - castToFloat(right.evalUniformVal(input), rightType));
            case DoubleType:
                return Double.doubleToRawLongBits(castToDouble(left.evalUniformVal(input), leftType) - castToDouble(right.evalUniformVal(input), rightType));
            default:
                throw new IllegalStateException();
        }
    }

    @Override
    public int evalInt(InternalRow input) {
        initType();
        return castToInt(left.evalUniformVal(input), leftType) - castToInt(right.evalUniformVal(input), rightType);
    }

    @Override
    public long evalLong(InternalRow input) {
        initType();
        return castToLong(left.evalUniformVal(input), leftType) - castToLong(right.evalUniformVal(input), rightType);
    }

    @Override
    public float evalFloat(InternalRow input) {
        initType();
        return castToFloat(left.evalUniformVal(input), leftType) - castToFloat(right.evalUniformVal(input), rightType);
    }

    @Override
    public double evalDouble(InternalRow input) {
        initType();
        return castToDouble(left.evalUniformVal(input), leftType) - castToDouble(right.evalUniformVal(input), rightType);
    }

    @Override
    public Expression withNewChildren(List<Expression> newChildren) {
        assert newChildren.size() == 2;
        return new Subtract(newChildren.get(0), newChildren.get(1));
    }

    @Override
    public List<Object> args() {return Lists.newArrayList(left, right);}
}
