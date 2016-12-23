package io.indexr.util;

import java.lang.reflect.Field;
import java.util.ArrayList;

public class InstanceTypeAssigner {
    private final Object instance;
    private final ArrayList<Class<?>> typeFieldClasses = new ArrayList<>();
    private final ArrayList<Field> typeFields = new ArrayList<>();

    public InstanceTypeAssigner(Object instance, Class<?> fieldType) {
        this.instance = instance;
        for (Field field : instance.getClass().getFields()) {
            Class<?> clazz = field.getType();
            if (fieldType.isAssignableFrom(clazz)) {
                typeFieldClasses.add(clazz);
                typeFields.add(field);
            }
        }
    }

    public boolean tryAssign(Object obj) {
        try {
            for (int i = 0; i < typeFieldClasses.size(); i++) {
                if (typeFieldClasses.get(i).isInstance(obj)
                        && typeFields.get(i).get(instance) == null) {
                    typeFields.get(i).set(instance, obj);
                    return true;
                }
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        return false;
    }
}
