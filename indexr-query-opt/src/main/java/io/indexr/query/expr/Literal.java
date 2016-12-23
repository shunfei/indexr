package io.indexr.query.expr;

import com.google.common.collect.Lists;

import org.apache.spark.unsafe.types.UTF8String;

import java.util.Collections;
import java.util.List;

import io.indexr.query.row.InternalRow;
import io.indexr.query.types.DataType;

public class Literal extends Expression {
    public final long uniformVal;
    public final String stringVal;
    public final DataType dataType;

    public Literal(long uniformVal, DataType dataType) {
        this(uniformVal, null, dataType);
    }

    public Literal(long uniformVal, String stringVal, DataType dataType) {
        this.uniformVal = uniformVal;
        this.stringVal = stringVal;
        this.dataType = dataType;
    }

    @Override
    public boolean foldable() {return true;}

    @Override
    public DataType dataType() {return dataType;}

    @Override
    public List<Expression> children() {return Collections.emptyList();}

    @Override
    public Expression withNewChildren(List<Expression> newChildren) {
        assert newChildren.size() == 0;
        return new Literal(uniformVal, stringVal, dataType);
    }

    @Override
    public List<Object> args() {
        return Lists.newArrayList(uniformVal, dataType);
    }

    @Override
    public long evalUniformVal(InternalRow input) {return uniformVal;}

    @Override
    public UTF8String evalString(InternalRow input) {return UTF8String.fromString(stringVal);}

    public static Literal _0L = new Literal(0, null, DataType.LongType);
    public static Literal _1L = new Literal(1, null, DataType.LongType);
}
