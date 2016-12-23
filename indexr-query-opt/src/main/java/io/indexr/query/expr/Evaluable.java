package io.indexr.query.expr;

import org.apache.spark.unsafe.types.UTF8String;

import io.indexr.query.row.InternalRow;
import io.indexr.query.types.DataType;
import io.indexr.query.types.TypeConverters;

public interface Evaluable {

    DataType dataType();

    long evalUniformVal(InternalRow input);

    /** Return generic value, for debug purpose. */
    default Object eval(InternalRow input) {
        return TypeConverters.cast(evalUniformVal(input), dataType());
    }

    default boolean evalBoolean(InternalRow input) {
        //assert dataType() == DataType.BooleanType;
        return evalUniformVal(input) != 0;
    }

    default int evalInt(InternalRow input) {
        //assert dataType() == DataType.IntegerType;
        return (int) evalUniformVal(input);
    }

    default long evalLong(InternalRow input) {
        //assert dataType() == DataType.LongType;
        return evalUniformVal(input);
    }

    default float evalFloat(InternalRow input) {
        //assert dataType() == DataType.FloatType;
        return (float) Double.longBitsToDouble(evalUniformVal(input));
    }

    default double evalDouble(InternalRow input) {
        //assert dataType() == DataType.DoubleType;
        return Double.longBitsToDouble(evalUniformVal(input));
    }

    default UTF8String evalString(InternalRow input) {
        assert dataType() == DataType.StringType;
        throw new UnsupportedOperationException();
    }
}
