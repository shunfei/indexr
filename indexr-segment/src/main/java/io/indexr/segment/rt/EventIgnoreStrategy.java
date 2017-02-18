package io.indexr.segment.rt;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.directory.api.util.Strings;

public enum EventIgnoreStrategy {
    @JsonProperty("no_ignore")
    NO_IGNORE(0),
    @JsonProperty("ignore_empty")
    IGNORE_EMPTY(1);

    public static final int ID_NO_IGNORE = 0;
    public static final int ID_IGNORE_EMPTY = 1;

    public final int id;

    private EventIgnoreStrategy(int id) {
        this.id = id;
    }

    public static EventIgnoreStrategy fromName(String name) {
        if (Strings.isEmpty(name)) {
            return NO_IGNORE;
        }
        for (EventIgnoreStrategy s : values()) {
            if (name.toUpperCase().equals(s.name())) {
                return s;
            }
        }
        throw new IllegalStateException("unsupported " + name);
    }
}
