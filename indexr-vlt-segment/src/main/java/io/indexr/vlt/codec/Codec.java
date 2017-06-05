package io.indexr.vlt.codec;

public interface Codec extends Encoder, Decoder {

    CodecType type();

    static int outputBufferSize(CodecType type, int valCount, int dataSize) {
        int size;
        switch (type) {
            case SIMPLE_DICT:
            case DICT_COMPRESS:
                size = dataSize + (valCount << 2) + 1024;
                break;
            case DICT:
                //size = dataSize + (valCount << 2) + 1024 + (1 << 17) + (1 << 16);
                size = dataSize + (valCount << 2) + 1024;
                break;
            default:
                if (valCount <= 64) {
                    size = dataSize << 1;
                } else {
                    size = (int) (dataSize * 1.5);
                }
        }
        // minimal 1024 bytes.
        return Math.max(1 << 10, size);
    }
}
