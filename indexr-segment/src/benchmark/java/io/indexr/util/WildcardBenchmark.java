package io.indexr.util;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.CharBuffer;

import io.indexr.io.ByteSlice;

@Fork(2)
@Measurement(iterations = 5)
@Warmup(iterations = 1)
public class WildcardBenchmark {
    static String p = "?ああ*あなたの妹*";
    static String s = "？ああgg你妹gg?G呵呵ooowfwef#%@#%@523424234$%^$%^$%^#%#%#$%#$^$^$%^#%##@$%^$%^$%^$%^$%##3tertgdgsdfwter345345345345345345345哒喵喵喵Gあなたの妹wefjfosdjfjwofjower2342#@%$#6#$^34^346\t\b";

    static CharBuffer scb;

    static {
        ByteSlice bs = ByteSlice.allocateDirect(s.length() * 2);
        UTF16Util.writeString(bs, 0, s);
        scb = bs.byteBuffer().asCharBuffer();
    }

    @Benchmark
    public void wildcard_String() {
        match(p, s);
    }

    @Benchmark
    public void wildcard_DirectCharBuffer() {
        match(p, scb);
    }

    private boolean match(CharSequence patern, CharSequence str) {
        return Wildcard.match(str, patern);
    }
}
