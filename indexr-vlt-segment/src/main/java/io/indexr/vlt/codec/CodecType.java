package io.indexr.vlt.codec;

public enum CodecType {
    PLAIN(1),
    DELTA(2),
    RLE(3),
    SIMPLE_DICT(4),
    /** This codec is private used only */
    DICT(5),
    DICT_COMPRESS(6),
    //
    ;

    public final int id;

    CodecType(int id) {
        this.id = id;
    }

    public static CodecType fromId(int id) {
        for (CodecType t : values()) {
            if (t.id == id) {
                return t;
            }
        }
        throw new RuntimeException("Illegal codec id: " + id);
    }
}
