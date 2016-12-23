package io.indexr.query.plan.physical;

import com.google.common.collect.Lists;

import org.apache.spark.memory.MemoryManager;
import org.apache.spark.sql.execution.UnsafeExternalRowSorter;
import org.apache.spark.util.collection.unsafe.sort.PrefixComparator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import io.indexr.query.expr.Expression;
import io.indexr.query.expr.SortOrder;
import io.indexr.query.expr.attr.Attribute;
import io.indexr.query.expr.attr.BoundReference;
import io.indexr.query.row.InternalRow;
import io.indexr.query.util.SortPrefixUtils;

public class Sort extends PPUnaryNode {
    public List<SortOrder> sortOrders;

    public Sort(List<SortOrder> sortOrders, PhysicalPlan child) {
        super(child);
        assert sortOrders.size() > 0;
        this.sortOrders = sortOrders;
    }

    @Override
    public List<Attribute> output() {
        return child.output();
    }

    @Override
    public List<Expression> expressions() {
        return new ArrayList<>(output());
    }

    @Override
    public PhysicalPlan withNewChildren(List<PhysicalPlan> newChildren) {
        assert newChildren.size() == 1;
        return new Sort(sortOrders, newChildren.get(0));
    }

    @Override
    public List<Object> args() {return Lists.newArrayList(sortOrders, child);}

    @Override
    protected Iterator<InternalRow> doExecute() {
        List<Attribute> childOutput = child.output();
        Comparator<InternalRow> ordering = newOrdering(sortOrders, childOutput);

        // The comparator for comparing prefix
        SortOrder boundSortExpression = BoundReference.bindReference(sortOrders.get(0), childOutput);
        PrefixComparator prefixComparator = SortPrefixUtils.getPrefixComparator(boundSortExpression);

        // The generator for prefix
        int prefixFiledOrdinal = ((BoundReference) boundSortExpression.child).ordinal;
        UnsafeExternalRowSorter.PrefixComputer prefixComputer = row -> row.getUniformVal(prefixFiledOrdinal);

        try {
            UnsafeExternalRowSorter sorter = new UnsafeExternalRowSorter(
                    schema(), ordering, prefixComparator, prefixComputer, MemoryManager.pageSize);
            return sorter.sort(child.execute());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
