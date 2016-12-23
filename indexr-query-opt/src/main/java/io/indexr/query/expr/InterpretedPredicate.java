package io.indexr.query.expr;

import java.util.List;
import java.util.function.Predicate;

import io.indexr.query.expr.attr.Attribute;
import io.indexr.query.expr.attr.BoundReference;
import io.indexr.query.row.InternalRow;

public class InterpretedPredicate {
    public static Predicate<InternalRow> create(Expression expression, List<Attribute> inputSchema) {
        return BoundReference.bindReference(expression, inputSchema)::evalBoolean;
    }

}
