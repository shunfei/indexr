package io.indexr.query.expr.project;

import java.util.List;

import io.indexr.query.expr.Expression;
import io.indexr.query.expr.attr.Attribute;
import io.indexr.query.expr.attr.BoundReference;
import io.indexr.query.row.GenericMutableRow;
import io.indexr.query.row.InternalRow;
import io.indexr.query.row.MutableRow;

public class InterpretedMutableProjection implements MutableProjection {
    private Expression[] expressions;
    private long[] buffer;
    private MutableRow mutableRow;

    public InterpretedMutableProjection(List<Expression> expressions, List<Attribute> inputSchema) {
        this.expressions = new Expression[expressions.size()];
        for (int i = 0; i < expressions.size(); i++) {
            this.expressions[i] = BoundReference.bindReference(expressions.get(i), inputSchema);
        }
        this.buffer = new long[expressions.size()];
        this.mutableRow = new GenericMutableRow(expressions.size());
    }

    @Override
    public InternalRow currentValue() {
        return mutableRow;
    }

    @Override
    public MutableProjection target(MutableRow row) {
        mutableRow = row;
        return this;
    }

    @Override
    public InternalRow apply(InternalRow row) {
        int i = 0;
        for (; i < expressions.length; i++) {
            // Store the result into buffer first, to make the projection atomic (needed by aggregation)
            buffer[i] = expressions[i].evalUniformVal(row);
        }
        for (i = 0; i < expressions.length; i++) {
            mutableRow.setUniformVal(i, buffer[i]);
        }
        return mutableRow;
    }
}
