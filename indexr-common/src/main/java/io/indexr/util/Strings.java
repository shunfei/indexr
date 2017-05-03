package io.indexr.util;

import java.util.Locale;

public final class Strings {
    private static final char[] TO_LOWER_CASE = new char[]{'\u0000', '\u0001', '\u0002', '\u0003', '\u0004', '\u0005', '\u0006', '\u0007', '\b', '\t', '\n', '\u000b', '\f', '\r', '\u000e', '\u000f', '\u0010', '\u0011', '\u0012', '\u0013', '\u0014', '\u0015', '\u0016', '\u0017', '\u0018', '\u0019', '\u001a', '\u001b', '\u001c', '\u001d', '\u001e', '\u001f', ' ', '!', '\"', '#', '$', '%', '&', '\'', '(', ')', '*', '+', ',', '-', '.', '/', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', ':', ';', '<', '=', '>', '?', '@', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '[', '\\', ']', '^', '_', '`', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '{', '|', '}', '~', '\u007f', '\u0080', '\u0081', '\u0082', '\u0083', '\u0084', '\u0085', '\u0086', '\u0087', '\u0088', '\u0089', '\u008a', '\u008b', '\u008c', '\u008d', '\u008e', '\u008f', '\u0090', '\u0091', '\u0092', '\u0093', '\u0094', '\u0095', '\u0096', '\u0097', '\u0098', '\u0099', '\u009a', '\u009b', '\u009c', '\u009d', '\u009e', '\u009f', ' ', '¡', '¢', '£', '¤', '¥', '¦', '§', '¨', '©', 'ª', '«', '¬', '\u00ad', '®', '¯', '°', '±', '²', '³', '´', 'µ', '¶', '·', '¸', '¹', 'º', '»', '¼', '½', '¾', '¿', 'À', 'Á', 'Â', 'Ã', 'Ä', 'Å', 'Æ', 'Ç', 'È', 'É', 'Ê', 'Ë', 'Ì', 'Í', 'Î', 'Ï', 'Ð', 'Ñ', 'Ò', 'Ó', 'Ô', 'Õ', 'Ö', '×', 'Ø', 'Ù', 'Ú', 'Û', 'Ü', 'Ý', 'Þ', 'ß', 'à', 'á', 'â', 'ã', 'ä', 'å', 'æ', 'ç', 'è', 'é', 'ê', 'ë', 'ì', 'í', 'î', 'ï', 'ð', 'ñ', 'ò', 'ó', 'ô', 'õ', 'ö', '÷', 'ø', 'ù', 'ú', 'û', 'ü', 'ý', 'þ', 'ÿ'};
    private static final char[] UPPER_CASE = new char[]{'\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '-', '\u0000', '\u0000', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000', '\u0000'};

    private Strings() {
    }


    public static String toHexString(byte[] res) {
        StringBuffer buf = new StringBuffer(res.length << 1);

        for (int ii = 0; ii < res.length; ++ii) {
            String digit = Integer.toHexString(255 & res[ii]);
            if (digit.length() == 1) {
                digit = '0' + digit;
            }

            buf.append(digit);
        }

        return buf.toString().toUpperCase();
    }

    public static byte[] toByteArray(String hexString) {
        int arrLength = hexString.length() >> 1;
        byte[] buf = new byte[arrLength];

        for (int ii = 0; ii < arrLength; ++ii) {
            int index = ii << 1;
            String digit = hexString.substring(index, index + 2);
            buf[ii] = (byte) Integer.parseInt(digit, 16);
        }

        return buf;
    }

    public static boolean isEmpty(String str) {
        return str == null || str.length() == 0;
    }

    public static boolean isEmpty(byte[] bytes) {
        return bytes == null || bytes.length == 0;
    }

    public static String trim(String str) {
        return isEmpty(str) ? "" : str.trim();
    }


    public static String trimLeft(String str) {
        if (isEmpty(str)) {
            return "";
        } else {
            int start = 0;

            for (int end = str.length(); start < end && str.charAt(start) == 32; ++start) {
                ;
            }

            return start == 0 ? str : str.substring(start);
        }
    }

    public static int trimLeft(char[] chars, int pos) {
        if (chars == null) {
            return pos;
        } else {
            while (pos < chars.length && chars[pos] == 32) {
                ++pos;
            }

            return pos;
        }
    }


    public static int trimLeft(byte[] bytes, int pos) {
        if (bytes == null) {
            return pos;
        } else {
            while (pos < bytes.length && bytes[pos] == 32) {
                ++pos;
            }

            return pos;
        }
    }

    public static String trimRight(String str) {
        if (isEmpty(str)) {
            return "";
        } else {
            int length = str.length();

            int end;
            for (end = length; end > 0 && str.charAt(end - 1) == 32 && (end <= 1 || str.charAt(end - 2) != 92); --end) {
                ;
            }

            return end == length ? str : str.substring(0, end);
        }
    }

    public static String trimRight(String str, int escapedSpace) {
        if (isEmpty(str)) {
            return "";
        } else {
            int length = str.length();

            int end;
            for (end = length; end > 0 && str.charAt(end - 1) == 32 && end > escapedSpace && (end <= 1 || str.charAt(end - 2) != 92); --end) {
                ;
            }

            return end == length ? str : str.substring(0, end);
        }
    }

    public static int trimRight(char[] chars, int pos) {
        if (chars == null) {
            return pos;
        } else {
            while (pos >= 0 && chars[pos - 1] == 32) {
                --pos;
            }

            return pos;
        }
    }

    public static char charAt(String string, int index) {
        if (string == null) {
            return '\u0000';
        } else {
            int length = string.length();
            return length != 0 && index >= 0 && index < length ? string.charAt(index) : '\u0000';
        }
    }

    public static byte byteAt(byte[] bytes, int index) {
        if (bytes == null) {
            return 0;
        } else {
            int length = bytes.length;
            return length != 0 && index >= 0 && index < length ? bytes[index] : 0;
        }
    }

    public static char charAt(char[] chars, int index) {
        if (chars == null) {
            return '\u0000';
        } else {
            int length = chars.length;
            return length != 0 && index >= 0 && index < length ? chars[index] : '\u0000';
        }
    }

    public static boolean equals(String str1, String str2) {
        return str1 == null ? str2 == null : str1.equals(str2);
    }

    public static String toLowerCase(String value) {
        if (null != value && value.length() != 0) {
            char[] chars = value.toCharArray();

            for (int i = 0; i < chars.length; ++i) {
                chars[i] = TO_LOWER_CASE[chars[i]];
            }

            return new String(chars);
        } else {
            return "";
        }
    }

    public static String toUpperCase(String value) {
        if (null != value && value.length() != 0) {
            char[] chars = value.toCharArray();

            for (int i = 0; i < chars.length; ++i) {
                chars[i] = UPPER_CASE[chars[i]];
            }

            return new String(chars);
        } else {
            return "";
        }
    }

    public static String upperCase(String str) {
        return str == null ? null : str.toUpperCase();
    }

    public static String lowerCase(String str) {
        return str == null ? null : str.toLowerCase(Locale.ENGLISH);
    }

    public static boolean isNotEmpty(String str) {
        return str != null && str.length() > 0;
    }
}
