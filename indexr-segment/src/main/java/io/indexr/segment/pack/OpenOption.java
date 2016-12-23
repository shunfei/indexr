package io.indexr.segment.pack;

public enum OpenOption {
    Overwrite(0),//
    ;

    public final int mark;

    private OpenOption(int bit) {
        this.mark = 0x01 << bit;
    }

    public boolean in(OpenOption... options) {
        for (OpenOption m : options) {
            if (m == this) {
                return true;
            }
        }
        return false;
    }
}