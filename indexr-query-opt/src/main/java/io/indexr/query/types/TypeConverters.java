package io.indexr.query.types;

public class TypeConverters {

    public static Object cast(long uniformVal, DataType dataType) {
        switch (dataType) {
            case BooleanType:
                return uniformVal != 0;
            case IntegerType:
                return (int) uniformVal;
            case LongType:
                return uniformVal;
            case FloatType:
                return (float) Double.longBitsToDouble(uniformVal);
            case DoubleType:
                return Double.longBitsToDouble(uniformVal);
            default:
                throw new IllegalStateException();
        }
    }

    // n * n
    // We will remove those code after migrate to code generation, don't worry!
    public static long castToUniformVal(long fromUniformVal, DataType from, DataType to) {
        if (from == to) {
            return fromUniformVal;
        }
        switch (to) {
            case BooleanType:
                return castToBool(fromUniformVal, from) ? 1 : 0;
            case IntegerType:
                return castToInt(fromUniformVal, from);
            case LongType:
                return castToLong(fromUniformVal, from);
            case FloatType:
                return Double.doubleToRawLongBits(castToFloat(fromUniformVal, from));
            case DoubleType:
                return Double.doubleToRawLongBits(castToDouble(fromUniformVal, from));
            default:
                throw new IllegalStateException();
        }
    }

    public static long castStringToUniformVal(String value, DataType to) {
        switch (to) {
            case BooleanType:
                return "FALSE".equalsIgnoreCase(value) ? 0 : 1;
            case IntegerType:
                return Integer.parseInt(value);
            case LongType:
                return Long.parseLong(value);
            case FloatType:
                return Double.doubleToRawLongBits(Float.parseFloat(value));
            case DoubleType:
                return Double.doubleToRawLongBits(Double.parseDouble(value));
            default:
                throw new IllegalStateException();
        }
    }

    public static boolean castToBool(long fromUniformVal, DataType from) {
        return fromUniformVal != 0;
    }

    public static int castToInt(long fromUniformVal, DataType from) {
        switch (from) {
            case BooleanType:
            case IntegerType:
            case LongType:
                return (int) fromUniformVal;
            case FloatType:
            case DoubleType:
                return (int) Double.longBitsToDouble(fromUniformVal);
            default:
                throw new IllegalStateException();
        }
    }

    public static long castToLong(long fromUniformVal, DataType from) {
        switch (from) {
            case BooleanType:
            case IntegerType:
            case LongType:
                return (long) fromUniformVal;
            case FloatType:
            case DoubleType:
                return (long) Double.longBitsToDouble(fromUniformVal);
            default:
                throw new IllegalStateException();
        }
    }

    public static float castToFloat(long fromUniformVal, DataType from) {
        switch (from) {
            case BooleanType:
            case IntegerType:
            case LongType:
                return (float) fromUniformVal;
            case FloatType:
            case DoubleType:
                return (float) Double.longBitsToDouble(fromUniformVal);
            default:
                throw new IllegalStateException();
        }
    }

    public static double castToDouble(long fromUniformVal, DataType from) {
        switch (from) {
            case BooleanType:
            case IntegerType:
            case LongType:
                return (double) fromUniformVal;
            case FloatType:
            case DoubleType:
                return Double.longBitsToDouble(fromUniformVal);
            default:
                throw new IllegalStateException();
        }
    }

    public static String castToString(long fromUniformVal, DataType from) {
        switch (from) {
            case BooleanType:
                return fromUniformVal == 0 ? "false" : "false";
            case IntegerType:
            case LongType:
                return String.valueOf(fromUniformVal);
            case FloatType:
                return String.valueOf((float) Double.longBitsToDouble(fromUniformVal));
            case DoubleType:
                return String.valueOf(Double.longBitsToDouble(fromUniformVal));
            default:
                throw new IllegalStateException();
        }
    }
}
