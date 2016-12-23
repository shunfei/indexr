package io.indexr.segment.rt;

import com.google.common.collect.Lists;

import org.apache.directory.api.util.Strings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.indexr.segment.ColumnSchema;
import io.indexr.segment.ColumnType;
import io.indexr.util.Trick;

public class RealtimeHelper {
    public static String validateSetting(List<ColumnSchema> columnSchemas,
                                         List<String> dims,
                                         List<Metric> metrics,
                                         boolean grouping) {
        int columnCount = columnSchemas.size();
        boolean hasDims = dims != null && dims.size() != 0;

        if (grouping) {
            if (!hasDims) {
                return "We need dims if grouping is enable!";
            }
        } else {
            if (metrics != null && metrics.size() > 0) {
                return "Metrics are useless when not grouping.";
            }
        }

        int i;
        if ((i = Trick.repeatIndex(columnSchemas, ColumnSchema::getName)) >= 0) {
            return String.format("column [%s] duplicated", columnSchemas.get(i).getName());
        }
        if ((i = Trick.repeatIndex(dims, d -> d)) >= 0) {
            return String.format("dim [%s] duplicated", dims.get(i));
        }
        if ((i = Trick.repeatIndex(metrics, m -> m.name)) >= 0) {
            return String.format("metric [%s] duplicated", metrics.get(i));
        }
        if (dims != null && metrics != null) {
            List<String> names = Trick.concatToList(dims, Lists.transform(metrics, m -> m.name));
            if ((i = Trick.repeatIndex(names, s -> s)) >= 0) {
                return String.format("[%s] can only be a dim or metric, not both", names.get(i));
            }
        }

        if (hasDims) {
            // If dims exists, we reorder the column, put dimensions before all metrics.
            Map<String, ColumnSchema> schemaMap = new HashMap<>(columnCount);
            for (ColumnSchema cs : columnSchemas) {
                schemaMap.put(cs.getName(), cs);
            }
            List<ColumnSchema> reorderingColumnSchemas = new ArrayList<>(columnCount);
            // Put dims first.
            for (String dim : dims) {
                ColumnSchema columnSchema = schemaMap.remove(dim);
                if (columnSchema == null) {
                    return "Dim not found : " + dim;
                }
                reorderingColumnSchemas.add(columnSchema);
            }
            // Now put metrics.
            if (grouping) {
                if (metrics != null) {
                    for (Metric metric : metrics) {
                        ColumnSchema columnSchema = schemaMap.remove(metric.name);
                        if (columnSchema == null) {
                            return "Metric not found : " + metric.name;
                        }
                        reorderingColumnSchemas.add(columnSchema);
                        if (!ColumnType.isNumber(columnSchema.getDataType())) {
                            return "metric field should be a number: " + metric.name;
                        }
                    }
                }
            } else {
                for (ColumnSchema columnSchema : schemaMap.values()) {
                    reorderingColumnSchemas.add(columnSchema);
                }
            }

            if (reorderingColumnSchemas.size() != columnCount) {
                for (ColumnSchema cs : columnSchemas) {
                    String name = cs.getName();
                    if (Trick.indexWhere(reorderingColumnSchemas, c -> Strings.equals(c.getName(), name)) < 0) {
                        return "Field must either be a dim or metric: " + name;
                    }
                }
                return String.format("Illegal setting. grouping: %s, columns: %s, dims: %s, metrics: %s",
                        grouping, columnSchemas, dims, metrics);
            }
        }
        return null;
    }
}
