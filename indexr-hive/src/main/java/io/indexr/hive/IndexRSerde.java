package io.indexr.hive;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.directory.api.util.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import io.indexr.segment.ColumnSchema;

public class IndexRSerde extends AbstractSerDe {
    private static final Log LOG = LogFactory.getLog(IndexRSerde.class);

    private StructObjectInspector objInspector;
    private SerDeStats stats;
    private long serdeSize;
    private List<String> columnNames;
    private List<TypeInfo> columnTypes;

    @Override
    public SerDeStats getSerDeStats() {
        stats.setRawDataSize(serdeSize);
        return stats;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return objInspector;
    }

    @Override
    public void initialize(Configuration conf, Properties tbl) throws SerDeException {
        String columnNameProperty = tbl.getProperty(IOConstants.COLUMNS);
        String columnTypeProperty = tbl.getProperty(IOConstants.COLUMNS_TYPES);

        if (Strings.isEmpty(columnNameProperty)) {
            columnNames = new ArrayList<String>();
        } else {
            columnNames = Arrays.asList(columnNameProperty.split(","));
        }
        if (Strings.isEmpty(columnTypeProperty)) {
            columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(StringUtils.repeat("string", ":", columnNames.size()));
        } else {
            columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
        }
        if (columnNames.size() != columnTypes.size()) {
            throw new IllegalArgumentException("IndexRHiveSerde initialization failed. Number of column " +
                    "name and column type differs. columnNames = " + columnNames + ", columnTypes = " +
                    columnTypes);
        }

        TypeInfo rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
        this.objInspector = new ArrayWritableObjectInspector((StructTypeInfo) rowTypeInfo);

        stats = new SerDeStats();
        serdeSize = 0;
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return ArrayWritable.class;
    }

    @Override
    public Writable serialize(Object obj, ObjectInspector objectInspector) throws SerDeException {
        if (!objectInspector.getCategory().equals(ObjectInspector.Category.STRUCT)) {
            throw new SerDeException("Cannot serialize " + objectInspector.getCategory() + ". Can only serialize a struct");
        }

        StructObjectInspector inspector = (StructObjectInspector) objectInspector;
        List<? extends StructField> fields = inspector.getAllStructFieldRefs();
        Writable[] arr = new Writable[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            StructField field = fields.get(i);
            Object subObj = inspector.getStructFieldData(obj, field);
            ObjectInspector subInspector = field.getFieldObjectInspector();
            arr[i] = createPrimitive(subObj, (PrimitiveObjectInspector) subInspector);
        }
        serdeSize = arr.length;
        return new ArrayWritable(Writable.class, arr);
    }

    private static Writable createPrimitive(Object obj, PrimitiveObjectInspector inspector)
            throws SerDeException {
        if (obj == null) {
            return null;
        }
        switch (inspector.getPrimitiveCategory()) {
            case DOUBLE:
                return new DoubleWritable(((DoubleObjectInspector) inspector).get(obj));
            case FLOAT:
                return new FloatWritable(((FloatObjectInspector) inspector).get(obj));
            case INT:
                return new IntWritable(((IntObjectInspector) inspector).get(obj));
            case LONG:
                return new LongWritable(((LongObjectInspector) inspector).get(obj));
            case STRING:
                return new Text(((StringObjectInspector) inspector).getPrimitiveJavaObject(obj));
            default:
                throw new SerDeException("Can't serialize primitive : " + inspector.getPrimitiveCategory());
        }
    }

    private ColumnSchema[] projectCols;
    private int[] validColIndexes;
    private int[] validColMapIds;
    private boolean isMapNeeded = true;

    @Override
    public Object deserialize(Writable writable) throws SerDeException {

        // Different segments could contain different schemas.
        // Especially the column orders could be different.
        // Here we re-map the column names to the real column ids.

        SchemaWritable reader = (SchemaWritable) writable;
        if (this.projectCols != reader.columns) {
            // Don't have to do it every time, only when schema is changed.
            mapColIndex(reader.columns);
            projectCols = reader.columns;
        }

        if (!isMapNeeded) {
            serdeSize = columnNames.size();
            return reader;
        } else {
            Writable[] projectWritables = reader.get();
            Writable[] writables = new Writable[columnNames.size()];
            for (int i = 0; i < validColIndexes.length; i++) {
                int colIndex = validColIndexes[i];
                int mapColId = validColMapIds[i];
                writables[colIndex] = projectWritables[mapColId];
            }

            serdeSize = validColIndexes.length;
            return new ArrayWritable(Writable.class, writables);
        }
    }

    private void mapColIndex(ColumnSchema[] columnSchemas) {
        int[] mapColIds = new int[columnNames.size()];
        boolean diff = false;
        int validColCount = 0;
        for (int i = 0; i < columnNames.size(); i++) {
            String name = columnNames.get(i);
            int mapColId = -1;
            for (int colId = 0; colId < columnSchemas.length; colId++) {
                ColumnSchema cs = columnSchemas[colId];
                if (cs.getName().equalsIgnoreCase(name)) {
                    mapColId = colId;
                    break;
                }
            }
            if (mapColId >= 0) {
                validColCount++;
            }
            mapColIds[i] = mapColId;
            if (mapColId != i) {
                diff = true;
            }
        }

        int[] validColIndexes = new int[validColCount];
        int[] validColMapIds = new int[validColCount];
        int vci = 0;
        for (int i = 0; i < mapColIds.length; i++) {
            int mapColId = mapColIds[i];
            if (mapColId >= 0) {
                int vcid = vci++;
                validColIndexes[vcid] = i;
                validColMapIds[vcid] = mapColId;
            }
        }

        this.validColIndexes = validColIndexes;
        this.validColMapIds = validColMapIds;
        this.isMapNeeded = diff;
    }
}
