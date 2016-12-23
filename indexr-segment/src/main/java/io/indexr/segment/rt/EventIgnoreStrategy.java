package io.indexr.segment.rt;

import org.apache.directory.api.util.Strings;

public class EventIgnoreStrategy {
    public static final int NO_IGNORE = 0;
    public static final int IGNORE_EMPTY = 1;

    public static int nameToId(String name) {
        if (Strings.isEmpty(name)) {
            return NO_IGNORE;
        }
        switch (name.toUpperCase()) {
            case "NO_IGNORE":
                return NO_IGNORE;
            case "IGNORE_EMPTY":
                return IGNORE_EMPTY;
            default:
                throw new IllegalStateException("unsupported " + name);
        }
    }

    public static String idToName(int id) {
        switch (id) {
            case NO_IGNORE:
                return "NO_IGNORE";
            case IGNORE_EMPTY:
                return "IGNORE_EMPTY";
            default:
                throw new IllegalStateException("unsupported " + id);
        }
    }
}
