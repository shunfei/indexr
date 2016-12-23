package io.indexr.util;

import com.google.common.base.Throwables;

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;

import java.io.UnsupportedEncodingException;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;

import io.indexr.io.ByteSlice;

public class UTF8Util {
    public static final String UTF8_NAME = "UTF-8";
    public static final Charset UTF8_CHARSET = Charset.forName("UTF-8");
    public static final int UTF8_MAX_BYTES_PER_CHAR = 3;
    public static final int UTF8_MAX_CHARS_PER_BYTE = 1;

    private final static ThreadLocal<SoftReference<CharsetEncoder>> encoder =
            new ThreadLocal<>();

    public static String fromUtf8(byte[] bytes) {
        try {
            return new String(bytes, UTF8_NAME);
        } catch (UnsupportedEncodingException e) {
            throw Throwables.propagate(e);
        }
    }

    public static String fromUtf8(byte[] bytes, int offset, int len) {
        try {
            return new String(bytes, offset, len, UTF8_NAME);
        } catch (UnsupportedEncodingException e) {
            throw Throwables.propagate(e);
        }
    }

    public static String fromUtf8(ByteBuffer byteBuffer) {
        try {
            return new String(byteBuffer.array(), byteBuffer.position(), byteBuffer.remaining(), UTF8_NAME);
        } catch (UnsupportedEncodingException e) {
            throw Throwables.propagate(e);
        }
    }

    public static String fromUtf8(ByteBuffer buffer, int size) {
        if (size == 0) {
            return "";
        }
        byte[] bytes = new byte[size];
        buffer.get(bytes);
        return fromUtf8(bytes);
    }

    public static String fromUtf8(ByteSlice buffer, int offset, int size) {
        if (size == 0) {
            return "";
        }

        byte[] bytes = new byte[size];
        buffer.get(offset, bytes);
        return fromUtf8(bytes);
    }

    public static byte[] toUtf8(CharSequence string) {
        if (string == null || string.length() == 0) {
            return new byte[]{};
        }
        try {
            return string.toString().getBytes(UTF8_NAME);
        } catch (UnsupportedEncodingException e) {
            throw Throwables.propagate(e);
        }
    }

    public static int utf8BufferByteLen(CharSequence str) {
        return str.length() * UTF8_MAX_BYTES_PER_CHAR;
    }

    private static CharsetEncoder getEncoder() {
        SoftReference<CharsetEncoder> sr = encoder.get();
        if (sr == null || sr.get() == null) {
            CharsetEncoder ce = UTF8_CHARSET.newEncoder();
            encoder.set(new SoftReference<CharsetEncoder>(ce));
            return ce;
        } else {
            return sr.get();
        }
    }

    /**
     * Faster version of {@link #toUtf8(CharSequence)} )}
     */
    public static void toUtf8(ByteBuffer buffer, String str) {
        if (str.length() == 0) {
            return;
        }
        CharsetEncoder ce = getEncoder();
        ce.reset();

        CharBuffer cb = CharBuffer.wrap(MemoryUtil.getStringValue(str));
        try {
            CoderResult cr = ce.encode(cb, buffer, true);
            if (!cr.isUnderflow())
                cr.throwException();
            cr = ce.flush(buffer);
            if (!cr.isUnderflow())
                cr.throwException();
        } catch (CharacterCodingException x) {
            // Substitution is always enabled,
            // so this shouldn't happen
            throw new Error(x);
        }
    }

    /**
     * According to {@link Long#parseLong(String)}.
     */
    public static long parseLong(byte[] data, int offset, int len)
            throws NumberFormatException {
        int radix = 10;
        long result = 0;
        boolean negative = false;
        //int i = 0, len = data.length;
        int i = offset;
        int to = offset + len;
        long limit = -Long.MAX_VALUE;
        long multmin;
        int digit;

        boolean ok = true;
        if (len > 0) {
            byte firstChar = data[i];
            if (firstChar < '0') { // Possible leading "+" or "-"
                if (firstChar == '-') {
                    negative = true;
                    limit = Long.MIN_VALUE;
                } else if (firstChar != '+')
                    throwNumberFormatException(data, offset, len);

                if (len == 1) // Cannot have lone "+" or "-"
                    throwNumberFormatException(data, offset, len);
                i++;
            }
            multmin = limit / radix;
            while (i < to) {
                // Accumulating negatively avoids surprises near MAX_VALUE
                digit = Character.digit(data[i++], radix);
                if (digit < 0) {
                    ok = false;
                    break;
                    //throwNumberFormatException(data, offset, len);
                }
                if (result < multmin) {
                    ok = false;
                    break;
                    //throwNumberFormatException(data, offset, len);
                }
                result *= radix;
                if (result < limit + digit) {
                    ok = false;
                    break;
                    //throwNumberFormatException(data, offset, len);
                }
                result -= digit;
            }
        } else {
            throwNumberFormatException(data, offset, len);
        }
        if (ok) {
            return negative ? result : -result;
        }

        // try floating point.
        return (long) parseDouble(data, offset, len);
    }

    // TODO need optimization.
    public static float parseFloat(byte[] data, int offset, int len) {
        try {
            return Float.parseFloat(new String(data, offset, len, UTF8_NAME));
        } catch (UnsupportedEncodingException e) {
            throw Throwables.propagate(e);
        }
    }

    // TODO need optimization.
    public static double parseDouble(byte[] data, int offset, int len) {
        try {
            return Double.parseDouble(new String(data, offset, len, UTF8_NAME));
        } catch (UnsupportedEncodingException e) {
            throw Throwables.propagate(e);
        }
    }

    private static void throwNumberFormatException(byte[] data, int offset, int len) {
        try {
            throw new NumberFormatException("For input string: \"" + new String(data, offset, len, UTF8_NAME) + "\"");
        } catch (UnsupportedEncodingException e) {
            throw Throwables.propagate(e);
        }
    }

    public static boolean containsCommaSep(byte[] bytes, byte[] sub) {
        return containsCommaSep(bytes, 0, bytes.length, sub);
    }

    /**
     * First split the utf-8 string by comma and trim blanks, then check whether they contains the sub string.
     */
    public static boolean containsCommaSep(byte[] bytes, int offset, int len, byte[] sub) {
        assert sub != null && sub.length > 0;

        int start = 0, lastBlankStart = -1;
        // 0: searching for next sub string
        // 1: recording sub string
        byte state = 0;

        int i = offset;
        for (; i < offset + len; i++) {
            //byte b = Platform.getByte(base, i);
            byte b = bytes[i];
            switch (state) {
                case 0:
                    switch (b) {
                        case ' ':
                            break;
                        case ',':
                            break;
                        default:
                            start = i;
                            state = 1;
                            lastBlankStart = -1;
                            break;
                    }
                    break;
                case 1:
                    switch (b) {
                        case ' ':
                            if (lastBlankStart == -1) {
                                lastBlankStart = i;
                            }
                            break;
                        case ',':
                            // check equals here.
                            int end = lastBlankStart == -1 ? i : lastBlankStart;
                            if (end - start == sub.length
                                    && ByteArrayMethods.arrayEquals(
                                    bytes, Platform.BYTE_ARRAY_OFFSET + start,
                                    sub, Platform.BYTE_ARRAY_OFFSET, sub.length)) {
                                return true;
                            }
                            start = 0;
                            state = 0;
                            break;
                        default:
                            // found a charactor which is not blank, reset black state.
                            lastBlankStart = -1;
                            break;
                    }
                default:
                    // Kidding me?
            }
        }
        if (state == 1) {
            int end = lastBlankStart == -1 ? i : lastBlankStart;
            if (end - start == sub.length
                    && ByteArrayMethods.arrayEquals(
                    bytes, Platform.BYTE_ARRAY_OFFSET + start,
                    sub, Platform.BYTE_ARRAY_OFFSET, sub.length)) {
                return true;
            }
        }
        return false;
    }


    /**
     * First split the utf-8 string by comma and trim blanks, then check whether they contains the sub string.
     */
    public static boolean containsCommaSep(Object base, long offset, int len, byte[] sub) {
        assert sub != null && sub.length > 0;

        long start = 0, lastBlankStart = -1;
        // 0: searching for next sub string
        // 1: recording sub string
        byte state = 0;

        long i = offset;
        for (; i < offset + len; i++) {
            byte b = Platform.getByte(base, i);
            switch (state) {
                case 0:
                    switch (b) {
                        case ' ':
                            break;
                        case ',':
                            break;
                        default:
                            start = i;
                            state = 1;
                            lastBlankStart = -1;
                            break;
                    }
                    break;
                case 1:
                    switch (b) {
                        case ' ':
                            if (lastBlankStart == -1) {
                                lastBlankStart = i;
                            }
                            break;
                        case ',':
                            // check equals here.
                            long end = lastBlankStart == -1 ? i : lastBlankStart;
                            if (end - start == sub.length
                                    && ByteArrayMethods.arrayEquals(
                                    base, start,
                                    sub, Platform.BYTE_ARRAY_OFFSET, sub.length)) {
                                return true;
                            }
                            start = 0;
                            state = 0;
                            break;
                        default:
                            // found a charactor which is not blank, reset black state.
                            lastBlankStart = -1;
                            break;
                    }
                default:
                    // Kidding me?
            }
        }
        if (state == 1) {
            long end = lastBlankStart == -1 ? i : lastBlankStart;
            if (end - start == sub.length
                    && ByteArrayMethods.arrayEquals(
                    base, start,
                    sub, Platform.BYTE_ARRAY_OFFSET, sub.length)) {
                return true;
            }
        }
        return false;
    }
}
