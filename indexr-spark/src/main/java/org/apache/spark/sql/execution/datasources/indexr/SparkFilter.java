package org.apache.spark.sql.execution.datasources.indexr;

import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import io.indexr.segment.ColumnSchema;
import io.indexr.segment.rc.Attr;
import io.indexr.segment.rc.Equal;
import io.indexr.segment.rc.Greater;
import io.indexr.segment.rc.GreaterEqual;
import io.indexr.segment.rc.Less;
import io.indexr.segment.rc.LessEqual;
import io.indexr.segment.rc.RCOperator;
import io.indexr.segment.rc.UnknownOperator;
import io.indexr.util.Trick;


public class SparkFilter {
    private static final Logger logger = LoggerFactory.getLogger(SparkFilter.class);

    public static RCOperator transform(List<ColumnSchema> schemas, List<Filter> filters) {
        List<Filter> validFilters = filters.stream().filter(f -> !(f instanceof IsNotNull)).collect(Collectors.toList());
        if (validFilters.size() == 0) {
            return null;
        }
        RCOperator op = new io.indexr.segment.rc.And(Trick.mapToList(validFilters, f -> transform(schemas, f)));
        op = op.optimize();
        return op;
    }

    private static RCOperator transform(List<ColumnSchema> schemas, Filter sparkFilter) {
        if (sparkFilter instanceof EqualTo) {
            EqualTo f = (EqualTo) sparkFilter;
            FilterParam param = parseFilterParam(schemas, f.attribute(), f.value());
            return param == null
                    ? new UnknownOperator(sparkFilter.toString())
                    : new Equal(param.attr, param.numValue, param.strValue);
        } else if (sparkFilter instanceof GreaterThan) {
            GreaterThan f = (GreaterThan) sparkFilter;
            FilterParam param = parseFilterParam(schemas, f.attribute(), f.value());
            return param == null
                    ? new UnknownOperator(sparkFilter.toString())
                    : new Greater(param.attr, param.numValue, param.strValue);
        } else if (sparkFilter instanceof GreaterThanOrEqual) {
            GreaterThanOrEqual f = (GreaterThanOrEqual) sparkFilter;
            FilterParam param = parseFilterParam(schemas, f.attribute(), f.value());
            return param == null
                    ? new UnknownOperator(sparkFilter.toString())
                    : new GreaterEqual(param.attr, param.numValue, param.strValue);
        } else if (sparkFilter instanceof LessThan) {
            LessThan f = (LessThan) sparkFilter;
            FilterParam param = parseFilterParam(schemas, f.attribute(), f.value());
            return param == null
                    ? new UnknownOperator(sparkFilter.toString())
                    : new Less(param.attr, param.numValue, param.strValue);
        } else if (sparkFilter instanceof LessThanOrEqual) {
            LessThanOrEqual f = (LessThanOrEqual) sparkFilter;
            FilterParam param = parseFilterParam(schemas, f.attribute(), f.value());
            return param == null
                    ? new UnknownOperator(sparkFilter.toString())
                    : new LessEqual(param.attr, param.numValue, param.strValue);
        } else if (sparkFilter instanceof In) {
            In f = (In) sparkFilter;
            FilterParam param = parseFilterParam(schemas, f.attribute(), f.values());
            return param == null
                    ? new UnknownOperator(sparkFilter.toString())
                    : new io.indexr.segment.rc.In(param.attr, param.numValues, param.strValues);
        } else if (sparkFilter instanceof And) {
            And f = (And) sparkFilter;
            return new io.indexr.segment.rc.And(Arrays.asList(transform(schemas, f.left()), transform(schemas, f.right())));
        } else if (sparkFilter instanceof Or) {
            Or f = (Or) sparkFilter;
            return new io.indexr.segment.rc.Or(Arrays.asList(transform(schemas, f.left()), transform(schemas, f.right())));
        } else if (sparkFilter instanceof Not) {
            Not f = (Not) sparkFilter;
            return new io.indexr.segment.rc.Not(transform(schemas, f.child()));
        } else {
            return new UnknownOperator(sparkFilter.toString());
        }
    }

    private static FilterParam parseFilterParam(List<ColumnSchema> schemas, String name, Object value) {
        //if (true) {
        //    throw new RuntimeException(String.format("class: %s, v: %s", value.getClass(), value));
        //}
        try {
            ColumnSchema columnSchema = Trick.find(schemas, s -> s.getName().equalsIgnoreCase(name));
            FilterParam param = new FilterParam();
            param.attr = new Attr(columnSchema.getName(), columnSchema.getSqlType());
            if (value instanceof Integer) {
                param.numValue = (Integer) value;
            } else if (value instanceof Long) {
                param.numValue = (Long) value;
            } else if (value instanceof Float) {
                param.numValue = Double.doubleToRawLongBits((Float) value);
            } else if (value instanceof Double) {
                param.numValue = Double.doubleToRawLongBits((Double) value);
            } else if (value instanceof String) {
                param.strValue = UTF8String.fromString((String) value);
            } else if (value instanceof Object[]) {
                List<Long> numValues = new ArrayList<>();
                List<UTF8String> strValues = new ArrayList<>();
                for (Object v : (Object[]) value) {
                    if (v instanceof Integer) {
                        numValues.add((long) (Integer) v);
                    } else if (v instanceof Long) {
                        numValues.add((Long) v);
                    } else if (v instanceof Float) {
                        numValues.add(Double.doubleToRawLongBits((Float) v));
                    } else if (v instanceof Double) {
                        numValues.add(Double.doubleToRawLongBits((Double) v));
                    } else if (v instanceof String) {
                        strValues.add(UTF8String.fromString((String) v));
                    } else {
                        return null;
                    }
                }
                if (!numValues.isEmpty()) {
                    param.numValues = toLongArray(numValues);
                } else {
                    param.strValues = strValues.toArray(new UTF8String[0]);
                }
            } else {
                return null;
            }
            return param;
        } catch (Exception e) {
            logger.warn("Failed to parse filter param value: {}", value, e);
            return null;
        }
    }

    private static long[] toLongArray(List<Long> list) {
        long[] array = new long[list.size()];
        for (int i = 0; i < list.size(); i++) {
            array[i] = list.get(i);
        }
        return array;
    }


    private static class FilterParam {
        Attr attr;

        long numValue;
        UTF8String strValue;

        long[] numValues;
        UTF8String[] strValues;
    }
}
