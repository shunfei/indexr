package io.indexr.util;

import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GenericCompression {
    public static final int SNAPPY = 0;
    public static final int GZIP = 1;

    public static final int CMP;

    static {
        String cmp = System.getProperty("io.indexr.compress.generic", "snappy");
        switch (cmp.toUpperCase()) {
            case "SNAPPY":
                CMP = SNAPPY;
                break;
            case "GZIP":
                CMP = GZIP;
                break;
            default:
                throw new RuntimeException("Illegal generic compression algorithm: " + cmp);
        }
    }


    public static byte[] compress(byte[] uncompressData) {
        try {
            switch (CMP) {
                case SNAPPY:
                    return Snappy.compress(uncompressData);
                case GZIP:
                    ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
                    GZIPOutputStream gzout = new GZIPOutputStream(out);
                    gzout.write(uncompressData, 0, uncompressData.length);
                    gzout.close();
                    return out.toByteArray();
                default:
                    throw new RuntimeException("Should not happen!");
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static byte[] decomress(byte[] compressedData) {
        try {
            switch (CMP) {
                case SNAPPY:
                    return Snappy.uncompress(compressedData);
                case GZIP:
                    ByteArrayInputStream in = new ByteArrayInputStream(compressedData);
                    GZIPInputStream gzin = new GZIPInputStream(in, compressedData.length);

                    ByteArrayOutputStream out = new ByteArrayOutputStream(10240);

                    byte[] buf = new byte[10240];
                    int res;
                    while ((res = gzin.read(buf)) >= 0) {
                        if (res > 0) {
                            out.write(buf, 0, res);
                        }
                    }

                    gzin.close();
                    return out.toByteArray();
                default:
                    throw new RuntimeException("Should not happen!");
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
