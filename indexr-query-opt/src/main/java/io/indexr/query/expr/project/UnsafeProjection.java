package io.indexr.query.expr.project;

import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import io.indexr.query.expr.Expression;
import io.indexr.query.expr.attr.Attribute;
import io.indexr.query.expr.attr.BoundReference;
import io.indexr.query.row.InternalRow;
import io.indexr.query.row.UnsafeRow;
import io.indexr.query.types.DataType;

public interface UnsafeProjection extends Projection {
    @Override
    UnsafeRow apply(InternalRow row);

    /**
     * Returns an UnsafeProjection for given StructType.
     */
    //def create(schema:StructType): UnsafeProjection = create(schema.fields.map(_.dataType))
    public static UnsafeProjection createFromSchema(StructType schema) {
        return create(schema.fields.stream().map(f -> f.dataType).collect(Collectors.toList()));
    }

    /**
     * Returns an UnsafeProjection for given Array of DataTypes.
     */
    public static UnsafeProjection create(List<DataType> fields) {
        ArrayList<Expression> boundRefs = new ArrayList<>();
        int ordinal = 0;
        for (DataType t : fields) {
            boundRefs.add(new BoundReference(ordinal++, t));
        }
        return createFromBoundedExprs(boundRefs);
    }

    /**
     * Returns an UnsafeProjection for given sequence of Expressions (bounded).
     */
    public static UnsafeProjection createFromBoundedExprs(List<? extends Expression> exprs) {
        return new InterpretedUnsafeProjection(exprs);
    }

    public static UnsafeProjection create(Expression expr) {
        return createFromBoundedExprs(Collections.singletonList(expr));
    }

    /**
     * Returns an UnsafeProjection for given sequence of Expressions, which will be bound to
     * `inputSchema`.
     */
    public static UnsafeProjection create(List<? extends Expression> exprs, List<Attribute> inputSchema) {
        return createFromBoundedExprs(exprs.stream().map(e -> BoundReference.bindReference(e, inputSchema)).collect(Collectors.toList()));
    }

    class InterpretedUnsafeProjection implements UnsafeProjection {
        List<? extends Expression> exprs;
        UnsafeRow result;
        byte[] buffer;

        public InterpretedUnsafeProjection(List<? extends Expression> exprs) {
            this.exprs = exprs;
            this.result = UnsafeRow.create8BytesFieldsRow(exprs.size());
        }

        @Override
        public UnsafeRow apply(InternalRow row) {
            int i = 0;
            for (Expression expr : exprs) {
                DataType dataType = expr.dataType();
                switch (dataType) {
                    case BooleanType:
                        result.setBoolean(i, expr.evalUniformVal(row) != 0);
                        break;
                    case IntegerType:
                        result.setInt(i, expr.evalInt(row));
                        break;
                    case LongType:
                        result.setLong(i, expr.evalLong(row));
                        break;
                    case FloatType:
                        result.setFloat(i, expr.evalFloat(row));
                        break;
                    case DoubleType:
                        result.setDouble(i, expr.evalDouble(row));
                        break;
                    case StringType:
                        result.setString(i, expr.evalString(row));
                        break;
                    default:
                        throw new IllegalStateException();
                }
                i++;
            }
            return result;
        }
    }

}
