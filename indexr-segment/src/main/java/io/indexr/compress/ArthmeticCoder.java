package io.indexr.compress;

import com.google.common.base.Preconditions;

import java.io.IOException;

import io.indexr.io.BitInputStream;
import io.indexr.io.BitOutputStream;


/**
 * Arithmetic coding.
 * 
 * <p> This code is based on <a href="https://github.com/nayuki/Arithmetic-Coding">Nayuki's
 * great code</a> .
 */
public abstract class ArthmeticCoder {
    static final long STATE_SIZE = 32;  // Number of bits for 'low' and 'high'. Must be in the range [1, 62] (and possibly more restricted).
    static final long MASK = (1L << (STATE_SIZE - 0)) - 1;  //  111...111, all ones
    static final long TOP_MASK = (1L << (STATE_SIZE - 1));      //  100...000, the top bit
    static final long SECOND_MASK = (1L << (STATE_SIZE - 2));      //  010...000, the next highest bit
    static final long MAX_RANGE = (1L << (STATE_SIZE - 0));      // 1000...000, maximum range during coding (trivial)
    static final long MIN_RANGE = (1L << (STATE_SIZE - 2)) + 2;  //   10...010, minimum range during coding (non-trivial)
    static final long MAX_TOTAL = Math.min(Long.MAX_VALUE / MAX_RANGE, MIN_RANGE);  // Maximum allowed total frequency at all times during coding


    long low, high;
    Frequency freq;

    ArthmeticCoder(Frequency freq) {
        low = 0;
        high = MASK;
        this.freq = freq;
    }

    void update(int symbol) throws IOException {
        long range = high - low + 1;

        // State check
        Preconditions.checkState(low < high && (low & MASK) == low && (high & MASK) == high, "Low or high out of range");
        Preconditions.checkState(range >= MIN_RANGE && range <= MAX_RANGE, "Range out of range");

        long total = freq.total();
        long symbolLow = freq.low(symbol);
        long symbolHigh = freq.high(symbol);
        Preconditions.checkState(symbolLow != symbolHigh, "Symbol has zero frequency");
        Preconditions.checkState(total < MAX_TOTAL, "Cannot code symbol because total is too large");

        // Update range
        long newLow = low + symbolLow * range / total;
        long newHigh = low + symbolHigh * range / total - 1;
        low = newLow;
        high = newHigh;

        // While the highest bits are equal
        while (((low ^ high) & TOP_MASK) == 0) {
            shift();
            low = (low << 1) & MASK;
            high = ((high << 1) & MASK) | 1;
        }

        // While the second highest bit of low is 1 and the second highest bit of high is 0
        while ((low & ~high & SECOND_MASK) != 0) {
            underflow();
            low = (low << 1) & (MASK >>> 1);
            high = ((high << 1) & (MASK >>> 1)) | TOP_MASK | 1;
        }
    }

    /**
     * Called when the top bit of low and high are equal.
     */
    abstract void shift() throws IOException;

    /**
     * Called when low=01xxxx and high=10yyyy.
     */
    abstract void underflow() throws IOException;

    /**
     * The frequency of symbols. Symbol must be ranged from <code>0</code> to <code>objCount-1</code>.
     */
    public static interface Frequency {
        /**
         * Then total number of symbol.
         */
        int symbolCount();

        /**
         * The total sum of symbol's frequency.
         */
        int total();

        /**
         * The total of the frequencies of all the symbols below the specified one.
         */
        int low(int symbol);

        /**
         * The total of the frequencies of the specified symbol and all the ones below.
         */
        int high(int symbol);
    }

    public static class SimpleFrequency implements Frequency {
        private int[] sums;
        private int total;
        private int symbolCount;


        public SimpleFrequency(int[] symbolCounts) {
            Preconditions.checkArgument(symbolCounts != null);
            Preconditions.checkArgument(symbolCounts.length > 0 && symbolCounts.length < (MASK >>> 2));

            symbolCount = symbolCounts.length;
            sums = new int[symbolCounts.length + 1];
            int lastSum = 0;
            for (int i = 0; i < symbolCounts.length; i++) {
                sums[i] = lastSum;
                lastSum += symbolCounts[i];
                sums[i + 1] = lastSum;
            }
            total = sums[sums.length - 1];
        }

        @Override
        public int symbolCount() {
            return symbolCount;
        }

        @Override
        public int total() {
            return total;
        }

        @Override
        public int low(int symbol) {
            return sums[symbol];
        }

        @Override
        public int high(int symbol) {
            return sums[symbol + 1];
        }
    }

    public static class Encoder extends ArthmeticCoder {
        private BitOutputStream outputStream;
        private int underFlowCount;

        Encoder(Frequency freq, BitOutputStream outputStream) throws IOException {
            super(freq);
            Preconditions.checkArgument(outputStream != null);
            this.outputStream = outputStream;
        }


        @Override
        void shift() throws IOException {
            int bit = (int) (low >>> (STATE_SIZE - 1));
            outputStream.write(bit);

            // Write out saved underflow bits
            while (underFlowCount > 0) {
                outputStream.write(bit ^ 0x01);
                underFlowCount--;
            }
        }

        @Override
        void underflow() {
            underFlowCount++;
        }

        /**
         * Must be called at the end of the stream of input symbols, otherwise the output data
         * cannot be decoded properly.
         */
        public void seal() throws IOException {
            outputStream.write(1);
            outputStream.seal();
        }

        public void write(int symbol) throws IOException {
            update(symbol);
        }
    }

    public static class Decoder extends ArthmeticCoder {
        private BitInputStream inputStream;
        private long code = 0;

        Decoder(Frequency freq, BitInputStream inputStream) throws IOException {
            super(freq);
            this.inputStream = inputStream;

            code = 0;
            for (int i = 0; i < STATE_SIZE; i++)
                code = code << 1 | readCodeBit();
        }

        private int readCodeBit() throws IOException {
            int b = inputStream.read();
            // Treat end of stream as an infinite number of trailing zeros
            return b != -1 ? b : 0;
        }

        @Override
        void shift() throws IOException {
            code = ((code << 1) & MASK) | readCodeBit();
        }

        @Override
        void underflow() throws IOException {
            code = (code & TOP_MASK) | ((code << 1) & (MASK >>> 1)) | readCodeBit();
        }

        public int read() throws IOException {
            // Translate from coding range scale to frequency table scale
            long total = freq.total();
            Preconditions.checkState(total < MAX_TOTAL, "Cannot decode symbol because total is too large");
            long range = high - low + 1;
            long offset = code - low;
            long value = ((offset + 1) * total - 1) / range;

            // A kind of binary search
            int start = 0;
            int end = freq.symbolCount();
            while (end - start > 1) {
                int middle = (start + end) >>> 1;
                if (freq.low(middle) > value)
                    end = middle;
                else
                    start = middle;
            }

            int symbol = start;
            update(symbol);
            return symbol;
        }
    }

}
