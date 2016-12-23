package io.indexr.query.plan.physical;

import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;

import io.indexr.query.expr.attr.Attribute;
import io.indexr.query.row.InternalRow;

public class LocalTableScan extends PPLeafNode {
    public List<Attribute> output;
    public List<InternalRow> rows;

    public LocalTableScan(List<Attribute> output, List<InternalRow> rows) {
        this.output = output;
        this.rows = rows;
    }

    @Override
    protected Iterator<InternalRow> doExecute() {return rows.iterator();}

    @Override
    public List<Attribute> output() {return output;}

    @Override
    public PhysicalPlan withNewChildren(List<PhysicalPlan> newChildren) {
        assert newChildren.size() == 0;
        return new LocalTableScan(output, rows);
    }

    @Override
    public List<Object> args() {return Lists.newArrayList(output, rows);}
}
