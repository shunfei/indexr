package io.indexr.segment.storage.itg;

public abstract class ColumnMeta {
    public int nameSize;
    public String name;
    public int sqlType;
    public boolean isIndexed;

    // @formatter:off
    public long dpnOffset(){return 0;};
    public long indexOffset() {return 0;}
    public long extIndexOffset() {return 0;}
    public long packOffset() {return 0;}
    // @formatter:on

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ColumnMeta info = (ColumnMeta) o;

        return nameSize == info.nameSize
                && sqlType == info.sqlType
                && isIndexed == info.isIndexed
                && dpnOffset() == info.dpnOffset()
                && indexOffset() == info.indexOffset()
                && extIndexOffset() == info.extIndexOffset()
                && packOffset() == info.packOffset()
                && (name != null ? name.equals(info.name) : info.name == null);
    }
}
