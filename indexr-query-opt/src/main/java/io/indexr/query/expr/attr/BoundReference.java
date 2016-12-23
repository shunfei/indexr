package io.indexr.query.expr.attr;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.spark.unsafe.types.UTF8String;

import java.util.Collections;
import java.util.List;

import io.indexr.query.expr.Expression;
import io.indexr.query.row.InternalRow;
import io.indexr.query.types.DataType;
import io.indexr.query.types.TypeConverters;

import static io.indexr.util.Trick.indexWhere;

public class BoundReference extends Expression {
    public int ordinal;
    public DataType dataType;

    public BoundReference(int ordinal, DataType dataType) {
        this.ordinal = ordinal;
        this.dataType = dataType;
    }

    @Override
    public String toString() {return String.format("input[%d, %s]", ordinal, dataType.simpleString());}

    @Override
    public DataType dataType() {return dataType;}

    @Override
    public List<Expression> children() {return Collections.emptyList();}

    @Override
    public Expression withNewChildren(List<Expression> newChildren) {return new BoundReference(ordinal, dataType);}

    @Override
    public List<Object> args() {return Lists.newArrayList(ordinal, dataType);}

    // @formatter:off
    @Override public Object eval(InternalRow input) {return TypeConverters.cast(evalUniformVal(input), dataType);}
    @Override public long evalUniformVal(InternalRow input) {return input.getUniformVal(ordinal);}
    @Override public int evalInt(InternalRow input) {return input.getInt(ordinal);}
    @Override public long evalLong(InternalRow input) {return input.getLong(ordinal);}
    @Override public float evalFloat(InternalRow input) {return input.getFloat(ordinal);}
    @Override public double evalDouble(InternalRow input) {return input.getDouble(ordinal);}
    @Override public UTF8String evalString(InternalRow input) {return input.getString(ordinal);}
    // @formatter:on

    @SuppressWarnings("unchecked")
    public static <A extends Expression> A bindReference(A expression, List<Attribute> input) {
        A res = (A) expression.transform(e -> {
            if (!(e instanceof AttributeReference)) {
                return e;
            }
            int ordinal = indexWhere(input, a -> a.exprId() == ((AttributeReference) e).exprId);
            Preconditions.checkState(ordinal >= 0, String.format("Couldn't find %s in %s}", e, input));
            return new BoundReference(ordinal, e.dataType());
        });

        return res;
    }
}
