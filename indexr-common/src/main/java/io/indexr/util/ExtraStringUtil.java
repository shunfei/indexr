package io.indexr.util;

import org.apache.commons.lang.StringUtils;

public class ExtraStringUtil {
    public static String trim(String s, String... c) {
        String res = s;
        for (String tc : c) {
            res = StringUtils.removeStart(StringUtils.removeEnd(res, tc), tc);
        }
        return res;
    }

    public static String trimAndSpace(String s, String... c) {
        return trim(s, c).trim();
    }
}