package io.indexr.segment.pack;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import io.indexr.query.row.InternalRow;
import io.indexr.segment.ColumnSchema;
import io.indexr.segment.SQLType;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.query.SegmentSelectHelper;
import io.indexr.util.DateTimeUtil;

import static io.indexr.util.ExtraStringUtil.trimAndSpace;
import static io.indexr.util.Trick.compareList;
import static io.indexr.util.Trick.indexLast;
import static io.indexr.util.Trick.indexWhere;

public class UpdateColSegment {

    private static void validateSchema(List<UpdateColSchema> columns) {
        for (int i = 0; i < columns.size(); i++) {
            UpdateColSchema acs = columns.get(i);
            int idx = indexLast(columns, c -> c.name.equalsIgnoreCase(acs.name));
            Preconditions.checkArgument(idx == i, String.format("Column [%s] duplicated!", acs.name));
        }
    }

    private static StorageColumn getStorageColumn(StorageSegment baseSegment,
                                                  UpdateColSchema newColumn,
                                                  int newColId,
                                                  Path storePath) throws IOException {
        SQLType sqlType = newColumn.sqlType;
        SegmentSchema baseSchema = baseSegment.schema();
        int colId = indexWhere(baseSchema.getColumns(), c -> c.getName().equals(trimAndSpace(newColumn.value)));
        if (colId >= 0 && baseSchema.getColumns().get(colId).getSqlType() == sqlType) {
            // This new column point to a existing column. We just need to move the column data.
            return baseSegment.column(colId).copy(-1, newColId, newColumn.name); // segmentId is useless here.
        }

        // Run the sql on baseSegment to get filed value and insert into newly created column.
        DPColumn dpColumn = new DPColumn(baseSegment.version(), newColId, newColumn.name, newColumn.sqlType, 0, storePath);
        dpColumn.initUpdate();
        SegmentSelectHelper.RowsConsumer consumer = it -> {
            while (it.hasNext()) {
                InternalRow row = it.next();
                switch (sqlType) {
                    case INT:
                        dpColumn.add(row.getInt(0));
                        break;
                    case BIGINT:
                        dpColumn.add(row.getLong(0));
                        break;
                    case FLOAT:
                        dpColumn.add(row.getFloat(0));
                        break;
                    case DOUBLE:
                        dpColumn.add(row.getDouble(0));
                        break;
                    case VARCHAR:
                        dpColumn.add(row.getString(0));
                        break;
                    case DATE:
                        dpColumn.add(DateTimeUtil.parseDate(row.getString(0).getBytes()));
                        break;
                    case TIME:
                        dpColumn.add(DateTimeUtil.parseTime(row.getString(0).getBytes()));
                        break;
                    case DATETIME:
                        dpColumn.add(DateTimeUtil.parseDateTime(row.getString(0).getBytes()));
                        break;
                    default:
                        throw new IllegalStateException("Illegal type :" + sqlType);
                }
            }
        };
        String sql = "SELECT " + newColumn.value + " FROM A";
        SegmentSelectHelper.selectSegment(baseSegment, sql, consumer);
        dpColumn.seal();

        return dpColumn;
    }

    /**
     * Use new columns schema for this segment.
     *
     * @param name               new segment name.
     * @param baseSegment        original segment.
     * @param targetColumnSchema columns the new segment will have.
     * @param storePath          the location where the new segment stores its newly added columns.
     * @return the new segment. If nothing is changed, returns the original segment instance.
     */
    public static StorageSegment newColumnSchema(String name,
                                                 StorageSegment baseSegment,
                                                 List<UpdateColSchema> targetColumnSchema,
                                                 Path storePath) throws IOException {
        validateSchema(targetColumnSchema);

        SegmentSchema baseSchema = baseSegment.schema();

        boolean equals = compareList(baseSchema.getColumns(), targetColumnSchema,
                (c1, c2) -> {
                    return c1.getName().equals(c2.name)
                            && c1.getSqlType() == c2.sqlType
                            && c1.getName().equals(trimAndSpace(c2.value));
                });
        if (equals) {
            return baseSegment;
        }

        SegmentSchema targetSchema = new SegmentSchema(
                targetColumnSchema.stream().map(c -> new ColumnSchema(c.name, c.sqlType)).collect(Collectors.toList()));

        int baseColumnCount = baseSchema.getColumns().size();
        StorageSegment.StorageColumnCreator<StorageColumn> scCreator = (ci, cs, rc) -> {
            return getStorageColumn(baseSegment, targetColumnSchema.get(ci), ci, storePath);
        };
        return new StorageSegment<StorageColumn>(baseSegment.version(), name, targetSchema, baseSegment.rowCount(), scCreator);
    }

    /**
     * Add some new columns to the segment. Those exsiting columns keep unchanged.
     *
     * @return the new segment. If nothing is changed, returns the original segment instance.
     */
    public static StorageSegment addColumn(String name,
                                           StorageSegment baseSegment,
                                           List<UpdateColSchema> newColumnSchema,
                                           Path storePath) throws IOException {
        validateSchema(newColumnSchema);

        List<UpdateColSchema> targetColumns = new ArrayList<>();
        for (ColumnSchema cs : baseSegment.schema().getColumns()) {
            targetColumns.add(new UpdateColSchema(cs.getName(), cs.getSqlType(), cs.getName()));
        }
        boolean update = false;
        for (UpdateColSchema acs : newColumnSchema) {
            if (indexWhere(targetColumns, c -> c.name.equals(acs.name)) < 0) {
                targetColumns.add(acs);
                update = true;
            }
        }
        if (!update) {
            return baseSegment;
        }
        return newColumnSchema(name, baseSegment, targetColumns, storePath);
    }

    /**
     * Remove some columns from the segment. Those nonexsitent columns will be ignored.
     *
     * @return the new segment. If nothing is changed, returns the original segment instance.
     */
    public static StorageSegment deleteColumn(String name,
                                              StorageSegment baseSegment,
                                              List<String> deleteColumns) throws IOException {
        List<UpdateColSchema> targetColumns = new ArrayList<>();
        boolean update = false;
        for (ColumnSchema cs : baseSegment.schema().getColumns()) {
            if (indexWhere(deleteColumns, c -> cs.getName().equals(c)) >= 0) {
                update = true;
            } else {
                targetColumns.add(new UpdateColSchema(cs.getName(), cs.getSqlType(), cs.getName()));
            }
        }
        if (!update) {
            return baseSegment;
        }
        return newColumnSchema(name, baseSegment, targetColumns, null);
    }


    /**
     * Alter columns in this segment, including the dataType and the value. Those nonexsitent columns will be ignored.
     *
     * @return the new segment. If nothing is changed, returns the original segment instance.
     */
    public static StorageSegment alterColumn(String name,
                                             StorageSegment baseSegment,
                                             List<UpdateColSchema> alterColumnSchema,
                                             Path storePath) throws IOException {
        validateSchema(alterColumnSchema);

        List<UpdateColSchema> targetColumns = new ArrayList<>();
        boolean update = false;
        for (ColumnSchema cs : baseSegment.schema().getColumns()) {
            int idx = indexWhere(alterColumnSchema, c -> c.name.equals(cs.getName()));
            if (idx < 0) {
                // Keep untouch.
                targetColumns.add(new UpdateColSchema(cs.getName(), cs.getSqlType(), cs.getName()));
            } else {
                targetColumns.add(alterColumnSchema.get(idx));
                update = true;
            }
        }
        if (!update) {
            return baseSegment;
        }

        return newColumnSchema(name, baseSegment, targetColumns, storePath);
    }
}
