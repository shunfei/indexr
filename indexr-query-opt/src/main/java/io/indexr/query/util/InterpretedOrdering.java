package io.indexr.query.util;

import java.util.Comparator;
import java.util.List;

import io.indexr.query.expr.SortOrder;
import io.indexr.query.expr.attr.Attribute;
import io.indexr.query.expr.attr.BoundReference;
import io.indexr.query.row.InternalRow;

import static io.indexr.util.Trick.mapToList;

public class InterpretedOrdering implements Comparator<InternalRow> {
    private SortOrder[] orders;

    public InterpretedOrdering(List<SortOrder> ordering, List<Attribute> inputSchema) {
        ordering = mapToList(ordering, so -> BoundReference.bindReference(so, inputSchema));
        orders = ordering.toArray(new SortOrder[ordering.size()]);
    }

    @Override
    public int compare(InternalRow r1, InternalRow r2) {
        for (SortOrder order : orders) {
            long left = order.child.evalUniformVal(r1);
            long right = order.child.evalUniformVal(r2);
            int comparison = order.dataType().comparator.compare(left, right);
            if (comparison != 0) {
                return order.isAscending() ? comparison : -comparison;
            }
        }
        return 0;
    }
}
