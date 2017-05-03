package io.indexr.segment;

import io.indexr.util.DateTimeUtil;
import io.indexr.util.Strings;
import io.indexr.util.UTF8Util;

public enum SQLType {
    INT(1, ColumnType.INT, "INT"),
    BIGINT(2, ColumnType.LONG, "BIGINT", "LONG"),
    FLOAT(3, ColumnType.FLOAT, "FLOAT"),
    DOUBLE(4, ColumnType.DOUBLE, "DOUBLE"),
    VARCHAR(5, ColumnType.STRING, "VARCHAR", "STRING"),

    DATE(6, ColumnType.LONG, "DATE"),
    TIME(7, ColumnType.INT, "TIME"),
    DATETIME(8, ColumnType.LONG, "DATETIME", "TIMESTAMP"),
    //
    ;


    public final int id;
    /** {@link ColumnType} */
    public final byte dataType;
    public final String[] names;

    SQLType(int id, byte dataType, String... names) {
        this.id = id;
        this.dataType = dataType;
        this.names = names;
    }

    public boolean isNumber() {
        return this != VARCHAR;
    }

    public static SQLType fromName(String name) {
        String upper = name.toUpperCase();
        for (SQLType type : values()) {
            for (String tname : type.names) {
                if (Strings.equals(tname, upper)) {
                    return type;
                }
            }
        }
        throw new IllegalStateException("Unsupported type: " + name);
    }

    public static SQLType fromId(int id) {
        for (SQLType type : values()) {
            if (id == type.id) {
                return type;
            }
        }
        throw new IllegalStateException("Illegal type id: " + id);
    }

    public static long parseNumber(SQLType sqlType, String strVal) {
        if (Strings.isEmpty(strVal)) {
            return 0;
        }
        switch (sqlType) {
            case INT:
                return Integer.parseInt(strVal);
            case BIGINT:
                return Long.parseLong(strVal);
            case FLOAT:
                return Double.doubleToLongBits(Float.parseFloat(strVal));
            case DOUBLE:
                return Double.doubleToLongBits(Double.parseDouble(strVal));
            case DATE:
                return DateTimeUtil.parseDate(UTF8Util.toUtf8(strVal));
            case TIME:
                return DateTimeUtil.parseTime(UTF8Util.toUtf8(strVal));
            case DATETIME:
                return DateTimeUtil.parseDateTime(UTF8Util.toUtf8(strVal));
            default:
                throw new RuntimeException("Not number type: " + sqlType);
        }
    }

    public static String toString(SQLType sqlType, long numVal) {
        switch (sqlType) {
            case INT:
                return String.valueOf((int) numVal);
            case BIGINT:
                return String.valueOf(numVal);
            case FLOAT:
                return String.valueOf((float) Double.longBitsToDouble(numVal));
            case DOUBLE:
                return String.valueOf(Double.longBitsToDouble(numVal));
            case DATE:
                return DateTimeUtil.getLocalDate(numVal).toString();
            case TIME:
                return DateTimeUtil.getLocalTime(numVal).toString();
            case DATETIME:
                return DateTimeUtil.getLocalDateTime(numVal).toString();
            default:
                throw new IllegalStateException("Illegal type: " + sqlType);
        }
    }
}
