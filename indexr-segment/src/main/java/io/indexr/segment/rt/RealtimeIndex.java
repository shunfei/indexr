package io.indexr.segment.rt;

import io.indexr.segment.ColumnSchema;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.pack.ColumnNode;

public class RealtimeIndex {
    private int colCount;
    private byte[] dateTypes;
    private long[] mins;
    private long[] maxs;
    private ColumnNode[] columnNodes;

    public RealtimeIndex(SegmentSchema schema) {
        this.colCount = schema.columns.size();
        this.columnNodes = new ColumnNode[colCount];
        this.dateTypes = new byte[colCount];
        int colId = 0;
        for (ColumnSchema cs : schema.columns) {
            dateTypes[colId] = cs.dataType;
            columnNodes[colId] = ColumnNode.none(cs.dataType);
            colId++;
        }
    }

    public void update(UTF8Row row) {
        for (int colId = 0; colId < colCount; colId++) {
            ColumnNode n = columnNodes[colId];
            byte dataType = dateTypes[colId];
            long v = row.getUniformValue(colId, dataType);
            n.minNumValue = ColumnNode.minNum(dataType, n.minNumValue, v);
            n.maxNumValue = ColumnNode.maxNum(dataType, n.maxNumValue, v);
        }
    }

    public ColumnNode columnNode(int colId) {
        return columnNodes[colId];
    }
}
