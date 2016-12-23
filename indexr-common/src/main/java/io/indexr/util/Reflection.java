package io.indexr.util;

import java.lang.reflect.Constructor;

public class Reflection {

    public static <T> Constructor<T> getConstructor(Class<T> clazz, Object... args) throws NoSuchMethodException {
        Class<?>[] argCls = new Class[args.length];
        for (int i = 0; i < args.length; i++) {
            argCls[i] = args[i].getClass();
        }
        return getConstructor(clazz, argCls);
    }

    public static <T> Constructor<T> getConstructor(Class<T> clazz, Class<?>... args) throws NoSuchMethodException {
        for (Constructor<?> constructor : clazz.getConstructors()) {
            Class<?>[] paramTypes = constructor.getParameterTypes();
            if (args.length != paramTypes.length) {
                continue;
            }
            boolean match = true;
            for (int i = 0; i < paramTypes.length; i++) {
                if (match) {
                    match = paramTypes[i].isAssignableFrom(args[i]);
                    if (!match && (paramTypes[i].isPrimitive() || args[i].isPrimitive())) {
                        match = castPrimitiveType(paramTypes[i]) == castPrimitiveType(args[i]);
                    }
                } else {
                    break;
                }
            }
            if (match) {
                return (Constructor<T>) constructor;
            }
        }
        // Try default java's method.
        return clazz.getDeclaredConstructor(args);
    }


    private static Class castPrimitiveType(Class c) {
        if (c == boolean.class) {
            return Boolean.class;
        } else if (c == char.class) {
            return Character.class;
        } else if (c == byte.class) {
            return Byte.class;
        } else if (c == short.class) {
            return Short.class;
        } else if (c == int.class) {
            return Integer.class;
        } else if (c == long.class) {
            return Long.class;
        } else if (c == float.class) {
            return Float.class;
        } else if (c == double.class) {
            return Double.class;
        } else if (c == void.class) {
            return Void.class;
        } else {
            return c;
        }
    }
}
