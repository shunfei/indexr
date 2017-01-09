package org.apache.spark.sql.types;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import io.indexr.query.expr.attr.Attribute;
import io.indexr.query.expr.attr.AttributeReference;
import io.indexr.query.types.AbstractDataType;
import io.indexr.query.types.DataType;

import static io.indexr.util.Trick.on;

/**
 * :: DeveloperApi ::
 * A [[StructType]] object can be constructed by
 * {{{
 * StructType(fields: Seq[StructField])
 * }}}
 * For a [[StructType]] object, one or multiple [[StructField]]s can be extracted by names.
 * If multiple [[StructField]]s are extracted, a [[StructType]] object will be returned.
 * If a provided name does not have a matching field, it will be ignored. For the case
 * of extracting a single StructField, a `null` will be returned.
 * Example:
 * {{{
 * import org.apache.spark.sql._
 * 
 * val struct =
 * StructType(
 * StructField("a", IntegerType, true) ::
 * StructField("b", LongType, false) ::
 * StructField("c", BooleanType, false) :: Nil)
 * 
 * // Extract a single StructField.
 * val singleField = struct("b")
 * // singleField: StructField = StructField(b,LongType,false)
 * 
 * // This struct does not have a field called "d". null will be returned.
 * val nonExisting = struct("d")
 * // nonExisting: StructField = null
 * 
 * // Extract multiple StructFields. Field names are provided in a set.
 * // A StructType object will be returned.
 * val twoFields = struct(Set("b", "c"))
 * // twoFields: StructType =
 * //   StructType(List(StructField(b,LongType,false), StructField(c,BooleanType,false)))
 * 
 * // Any names without matching fields will be ignored.
 * // For the case shown below, "d" will be ignored and
 * // it is treated as struct(Set("b", "c")).
 * val ignoreNonExisting = struct(Set("b", "c", "d"))
 * // ignoreNonExisting: StructType =
 * //   StructType(List(StructField(b,LongType,false), StructField(c,BooleanType,false)))
 * }}}
 * 
 * A [[org.apache.spark.sql.Row]] object is used as a value of the StructType.
 * Example:
 * {{{
 * import org.apache.spark.sql._
 * 
 * val innerStruct =
 * StructType(
 * StructField("f1", IntegerType, true) ::
 * StructField("f2", LongType, false) ::
 * StructField("f3", BooleanType, false) :: Nil)
 * 
 * val struct = StructType(
 * StructField("a", innerStruct, true) :: Nil)
 * 
 * // Create a Row with the schema defined by struct
 * val row = Row(Row(1, 2, true))
 * // row: Row = [[1,2,true]]
 * }}}
 */
public class StructType implements AbstractDataType {
    public final List<StructField> fields;

    public StructType(List<StructField> fields) {
        this.fields = fields;
    }

    public int size() {
        return fields.size();
    }

    public StructField get(int ordinal) {
        return fields.get(ordinal);
    }

    public List<StructField> fields() {
        return fields;
    }

    @Override
    public int defaultSize() {
        return fields.stream().map(f -> f.dataType.defaultSize()).reduce(0, Integer::sum);
    }

    /** Returns all field names in an array. */
    public List<String> fieldNames() {
        return fields.stream().map(f -> f.name).collect(Collectors.toList());
    }

    public StructType add(String name, DataType dataType, Metadata metadata) {
        return new StructType(on(
                new ArrayList<>(fields.size() + 1),
                ArrayList::addAll, fields,
                ArrayList::add, new StructField(name, dataType, metadata)));
    }

    public StructType add(String name, String dataType, Metadata metadata) {
        return add(name, DataType.fromName(dataType), metadata);
    }

    public List<Attribute> toAttributes() {
        return fields.stream().map(f -> new AttributeReference(f.name, f.dataType)).collect(Collectors.toList());
    }

    public static StructType fromAttributes(List<? extends Attribute> attributes) {
        return new StructType(attributes.stream().map(a -> new StructField(a.name(), a.dataType())).collect(Collectors.toList()));
    }
}

