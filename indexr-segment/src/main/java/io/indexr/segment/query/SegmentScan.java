package io.indexr.segment.query;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import org.apache.spark.unsafe.types.UTF8String;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import io.indexr.query.expr.attr.Attribute;
import io.indexr.query.plan.physical.PPLeafNode;
import io.indexr.query.plan.physical.PhysicalPlan;
import io.indexr.query.row.GenericMutableRow;
import io.indexr.query.row.InternalRow;
import io.indexr.query.types.DataType;
import io.indexr.query.types.TypeConverters;
import io.indexr.segment.Row;
import io.indexr.segment.Segment;

public class SegmentScan extends PPLeafNode {
    public final List<Attribute> output;
    public final List<Segment> segments;

    private final byte[] dataTypes;

    public SegmentScan(List<Attribute> output, List<Segment> segments) {
        // FIXME We assume that all those segments have the same attributes as ouput. Which is NOT!
        this.segments = segments;
        this.output = output;

        this.dataTypes = new byte[output.size()];
        for (int i = 0; i < dataTypes.length; i++) {
            dataTypes[i] = SegmentSelectHelper.queryTypeToStorageType(output.get(i).dataType());
        }
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    protected Iterator<InternalRow> doExecute() {
        Iterator<Iterator<Row>> its = segments.stream().map(s -> s.rowTraversal().iterator()).iterator();
        Iterator<Row> segmentItr = Iterators.concat(its);
        SegRowToQueryRow row = new SegRowToQueryRow();
        return new Iterator<InternalRow>() {
            @Override
            public boolean hasNext() {
                return segmentItr.hasNext();
            }

            @Override
            public InternalRow next() {
                return row.apply(segmentItr.next());
            }
        };
    }

    @Override
    public PhysicalPlan withNewChildren(List<PhysicalPlan> newChildren) {
        assert newChildren.size() == 0;
        return new SegmentScan(output, segments);
    }

    @Override
    public List<Object> args() {return Lists.newArrayList(output, segments);}

    private GenericMutableRow createRow(Row row) {
        GenericMutableRow mrow = new GenericMutableRow(output.size());
        for (int coldId = 0; coldId < output.size(); coldId++) {
            Attribute attr = output.get(coldId);
            if (attr.dataType() == DataType.StringType) {
                mrow.setString(coldId, row.getString(coldId));
            } else {
                mrow.setUniformVal(coldId, row.getUniformValue(coldId, dataTypes[coldId]));
            }
        }
        return mrow;
    }

    private class SegRowToQueryRow extends InternalRow implements Function<Row, InternalRow> {
        int numFields = output.size();
        Row curRow;

        @Override
        public InternalRow apply(Row row) {
            curRow = row;
            return this;
        }

        @Override
        public int numFields() {
            return numFields;
        }

        @Override
        public InternalRow copy() {
            GenericMutableRow row = new GenericMutableRow(numFields);
            for (int coldId = 0; coldId < numFields; coldId++) {
                row.setUniformVal(coldId, curRow.getUniformValue(coldId, dataTypes[coldId]));
            }
            return row;
        }

        // @formatter:off
        @Override public boolean getBoolean(int ordinal) {return curRow.getUniformValue(ordinal, dataTypes[ordinal]) != 0;}
        @Override public int getInt(int ordinal) {return curRow.getInt(ordinal);}
        @Override public long getLong(int ordinal) {return curRow.getLong(ordinal);}
        @Override public float getFloat(int ordinal) {return curRow.getFloat(ordinal);}
        @Override public double getDouble(int ordinal) {return curRow.getDouble(ordinal);}
        @Override public long getUniformVal(int ordinal) {return curRow.getUniformValue(ordinal, dataTypes[ordinal]);}
        @Override public UTF8String getString(int ordinal) {return curRow.getString(ordinal);}
        @Override public Object get(int ordinal, DataType dataType) {return TypeConverters.cast(curRow.getUniformValue(ordinal, dataTypes[ordinal]), dataType);}
        // @formatter:on
    }

}
