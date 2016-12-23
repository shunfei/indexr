package io.indexr.query.expr.project;

import io.indexr.query.row.InternalRow;
import io.indexr.query.row.MutableRow;

public interface MutableProjection extends Projection {
    InternalRow currentValue();

    MutableProjection target(MutableRow row);
}
