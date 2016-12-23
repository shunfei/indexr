package io.indexr.query;

@FunctionalInterface
public interface Resolver {
    boolean resolve(String first, String second);

    public static Resolver caseInsensitiveResolution = String::equalsIgnoreCase;
    public static Resolver caseSensitiveResolution = String::equals;
}
