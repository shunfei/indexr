package io.indexr.segment;

public enum SystemConfig {
    FAILFAST("indexr.failfast", "true");

    public final String key;
    public final String defaultValue;

    private SystemConfig(String key, String defaultValue) {
        this.key = key;
        this.defaultValue = defaultValue;
    }

    public boolean getBool() {
        return Boolean.parseBoolean(System.getProperty(key, defaultValue));
    }

    public int getInt() {
        return Integer.parseInt(System.getProperty(key, defaultValue));
    }

    public long getLong() {
        return Long.parseLong(System.getProperty(key, defaultValue));
    }

    public float getFloat() {
        return Float.parseFloat(System.getProperty(key, defaultValue));
    }

    public double getDouble() {
        return Double.parseDouble(System.getProperty(key, defaultValue));
    }

    public String get() {
        return System.getProperty(key, defaultValue);
    }
}
