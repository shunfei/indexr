package io.indexr.query.expr.project;

import io.indexr.query.row.InternalRow;

@FunctionalInterface
public interface Projection {
    InternalRow apply(InternalRow input);
}
