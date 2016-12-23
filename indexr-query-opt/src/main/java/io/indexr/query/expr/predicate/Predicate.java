package io.indexr.query.expr.predicate;

import io.indexr.query.expr.Expression;
import io.indexr.query.row.InternalRow;
import io.indexr.query.types.DataType;

public abstract class Predicate extends Expression {
    @Override
    public final DataType dataType() {
        return DataType.BooleanType;
    }

    @Override
    public final long evalUniformVal(InternalRow input) {
        return evalBoolean(input) ? 1 : 0;
    }

    @Override
    public abstract boolean evalBoolean(InternalRow input);
}
