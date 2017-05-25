package io.indexr.segment.storage.itg;

public abstract class ColumnMeta {
    public int nameSize;
    public String name;
    public int sqlType;
    public boolean isIndexed; // means outer index

    // @formatter:off
    public abstract long dpnOffset();
    public abstract long indexOffset();
    public abstract long extIndexOffset();
    public abstract long outerIndexOffset();
    public abstract long outerIndexSize();
    public abstract long packOffset();
    // @formatter:on

    public abstract boolean equals(int version, ColumnMeta o);
}
