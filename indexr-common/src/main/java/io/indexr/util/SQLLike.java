package io.indexr.util;

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Escape charactor is \
 * TODO the _ matcher is broken for UTF-8 right now.
 */
public class SQLLike {

    public static boolean match(UTF8String string, UTF8String pattern) {
        return match(string, pattern, 0, 0);
    }

    private static byte getByte(UTF8String str, int i) {
        return Platform.getByte(str.getBaseObject(), str.getBaseOffset() + i);
    }

    /**
     * Internal matching recursive function.
     */
    private static boolean match(UTF8String string, UTF8String pattern, int sNdx, int pNdx) {
        int pLen = pattern.numBytes();
        if (pLen == 1) {
            if (getByte(pattern, 0) == '%') {     // speed-up
                return true;
            }
        }
        int sLen = string.numBytes();
        boolean nextIsNotWildcard = false;

        while (true) {

            // check if end of string and/or pattern occurred
            if ((sNdx >= sLen)) {        // end of string still may have pending '*' in pattern
                while ((pNdx < pLen) && (getByte(pattern, pNdx) == '%')) {
                    pNdx++;
                }
                return pNdx >= pLen;
            }
            if (pNdx >= pLen) {                    // end of pattern, but not end of the string
                return false;
            }
            byte p = getByte(pattern, pNdx);        // pattern char

            // perform logic
            if (!nextIsNotWildcard) {

                if (p == '\\') {
                    pNdx++;
                    nextIsNotWildcard = true;
                    continue;
                }
                if (p == '_') {
                    sNdx++;
                    pNdx++;
                    continue;
                }
                if (p == '%') {
                    byte pNext = 0;                        // next pattern char
                    if (pNdx + 1 < pLen) {
                        pNext = getByte(pattern, pNdx + 1);
                    }
                    if (pNext == '%') {                    // double '*' have the same effect as one '*'
                        pNdx++;
                        continue;
                    }
                    int i;
                    pNdx++;

                    // find recursively if there is any substring from the end of the
                    // line that matches the rest of the pattern !!!
                    for (i = string.numBytes(); i >= sNdx; i--) {
                        if (match(string, pattern, i, pNdx)) {
                            return true;
                        }
                    }
                    return false;
                }
            } else {
                nextIsNotWildcard = false;
            }

            // check if pattern char and string char are equals
            if (p != getByte(string, sNdx)) {
                return false;
            }

            // everything matches for now, continue
            sNdx++;
            pNdx++;
        }
    }
}
