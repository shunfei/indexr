package io.indexr.segment;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.directory.api.util.Strings;

public enum SegmentMode {
    @JsonProperty("balance")
    BALANCE(0, true, true),
    @JsonProperty("storage")
    STORAGE(1, true, false),
    @JsonProperty("performance")
    PERFORMANCE(2, false, false);

    public final int id;
    public final boolean compress;
    public final boolean useExtIndex;

    public static final SegmentMode DEFAULT = BALANCE;

    SegmentMode(int id, boolean compress, boolean useExtIndex) {
        this.id = id;
        this.compress = compress;
        this.useExtIndex = useExtIndex;
    }

    @Override
    public String toString() {
        return name().toLowerCase();
    }

    public static SegmentMode fromId(int id) {
        for (SegmentMode mode : values()) {
            if (id == mode.id) {
                return mode;
            }
        }
        throw new RuntimeException("illegal segment mode id: " + id);
    }

    public static SegmentMode fromName(String name) {
        if (Strings.isEmpty(name)) {
            return DEFAULT;
        }
        for (SegmentMode mode : values()) {
            if (name.toUpperCase().equals(mode.name())) {
                return mode;
            }
        }
        throw new RuntimeException("illegal segment mode: " + name);
    }

    public static SegmentMode fromNameWithCompress(String name, boolean compress) {
        if (Strings.isEmpty(name)) {
            return compress ? BALANCE : PERFORMANCE;
        }
        return fromName(name);
    }

    public static SegmentMode fromNameWithCompress(String name, Boolean compress) {
        return fromNameWithCompress(name, compress == null ? DEFAULT.compress : compress);
    }
}
