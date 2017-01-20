package io.indexr.segment.pack;

import java.io.IOException;
import java.util.Arrays;

import io.indexr.io.ByteBufferReader;

public enum Version {
    VERSION_0(0, new byte[]{'I', 'X', 'R', 'S', 'E', 'G', '0', '1'}),
    VERSION_1(1, new byte[]{'I', 'X', 'R', 'S', 'E', 'G', '0', '2'}),
    VERSION_2(2, new byte[]{'I', 'X', 'R', 'S', 'E', 'G', '0', '3'}),
    // VERSION_3 was eaten by a big dog.
    VERSION_4(4, new byte[]{'I', 'X', 'R', 'S', 'E', 'G', '0', '4'}),
    //
    ;

    // Faster access.
    public static final int VERSION_0_ID = 0;
    public static final int VERSION_1_ID = 1;
    public static final int VERSION_2_ID = 2;
    public static final int VERSION_4_ID = 4;

    public static final int INDEXR_SEG_FILE_FLAG_SIZE = 8;
    public static final Version LATEST = VERSION_4;
    public static final int LATEST_ID = LATEST.id;

    public final int id;
    public final byte[] flag;

    Version(int id, byte[] flag) {
        this.id = id;
        this.flag = flag;
    }

    public static Version fromId(int versionId) {
        for (Version v : values()) {
            if (v.id == versionId) {
                return v;
            }
        }
        return null;
    }

    public static Version check(ByteBufferReader reader) throws IOException {
        if (!reader.exists(INDEXR_SEG_FILE_FLAG_SIZE - 1)) {
            return null;
        }
        byte[] magic = reader.read(0, INDEXR_SEG_FILE_FLAG_SIZE);
        for (Version v : Version.values()) {
            if (Arrays.equals(magic, v.flag)) {
                return v;
            }
        }
        return null;
    }
}
