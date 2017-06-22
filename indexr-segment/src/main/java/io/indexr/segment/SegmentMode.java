package io.indexr.segment;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import io.indexr.plugin.Plugins;
import io.indexr.segment.storage.VersionAdapter;
import io.indexr.segment.storage.VersionAdapter_Basic;
import io.indexr.util.Strings;

public class SegmentMode implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final SegmentMode BASIC = new SegmentMode(new int[]{0, 1, 2}, new String[]{"basic", "balance", "storage", "performance"}, new VersionAdapter_Basic());
    // Inner used.
    public static final SegmentMode FAST = new SegmentMode(new int[]{3}, new String[]{"fast"}, new VersionAdapter_Basic(true));
    public static SegmentMode DEFAULT;

    private static final ArrayList<SegmentMode> modes = new ArrayList<>();

    public static void addMode(SegmentMode mode) {
        modes.add(mode);
    }

    public static void setDefault(SegmentMode mode) {
        DEFAULT = mode;
    }

    static {
        addMode(BASIC);
        addMode(FAST);

        setDefault(BASIC);

        try {
            Plugins.loadPlugins();
        } catch (Exception e) {
            throw new RuntimeException("Load plugins failed", e);
        }
    }

    public final int id;
    public final int[] aliasIds;
    public final String name;
    public final String[] aliasNames;
    public final VersionAdapter versionAdapter;

    public SegmentMode(int[] aliasIds, String[] aliasNames, VersionAdapter versionAdapter) {
        this.id = aliasIds[0];
        this.aliasIds = aliasIds;
        this.name = aliasNames[0];
        this.aliasNames = aliasNames;
        this.versionAdapter = versionAdapter;
    }

    public SegmentMode(int id, String name, VersionAdapter versionAdapter) {
        this(new int[]{id}, new String[]{name}, versionAdapter);
    }

    public String name() {
        return name;
    }

    public static List<SegmentMode> values() {
        return modes;
    }

    @Override
    public boolean equals(Object obj) {
        SegmentMode other = (SegmentMode) obj;
        return id == other.id && Strings.equals(name, other.name);
    }

    @Override
    public String toString() {
        return name().toLowerCase();
    }

    public static SegmentMode fromId(int id) {
        for (SegmentMode mode : values()) {
            if (id == mode.id || containsInt(mode.aliasIds, id)) {
                return mode;
            }
        }
        throw new RuntimeException("Unsupported segment mode id: " + id);
    }

    public static SegmentMode fromName(String name) {
        if (Strings.isEmpty(name)) {
            return DEFAULT;
        }
        for (SegmentMode mode : values()) {
            for (String n : mode.aliasNames) {
                if (n.equalsIgnoreCase(name)) {
                    return mode;
                }
            }
        }
        throw new RuntimeException("Unsupported segment mode: " + name);
    }

    public static SegmentMode fromNameWithCompress(String name, boolean compress) {
        if (Strings.isEmpty(name)) {
            return DEFAULT;
        }
        return fromName(name);
    }

    public static SegmentMode fromNameWithCompress(String name, Boolean compress) {
        return fromNameWithCompress(name, compress == null);
    }

    private static boolean containsInt(int[] arr, int v) {
        for (int i : arr) {
            if (i == v) return true;
        }
        return false;
    }
}
