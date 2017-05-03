package io.indexr.util;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.BitSetIterator;

/**
 * A wrapper of the underlying bitmap, only support [0, 65536).
 *
 * We guarantee that the content of {@link #SOME}, {@link #ALL} or {@link #NONE} will never be changed.
 * As those static instances are shared around.
 */
public class BitMap {
    public static final int CAPACITY = 65536;
    public static final BitMap SOME = new BitMap(CAPACITY);
    public static final BitMap ALL = new BitMap(CAPACITY);
    public static final BitMap NONE = new BitMap(0);

    static {
        SOME.agent.flip(0, CAPACITY);
        ALL.agent.flip(0, CAPACITY);
    }

    private final BitSet agent;

    public BitMap() {
        this(CAPACITY);
    }

    public BitMap(int bits) {
        assert bits <= CAPACITY;
        agent = new BitSet(bits);
    }

    public void set(int id) {
        assert this != SOME;
        assert this != ALL;
        assert this != NONE;

        agent.set(id);
    }

    public void unset(int start, int end) {
        assert this != SOME;
        assert this != ALL;
        assert this != NONE;

        agent.clear(start, end);
    }

    public boolean get(int id) {
        return agent.get(id);
    }

    public int cardinality() {
        return (int) agent.cardinality();
    }

    public BitSetIterator iterator() {
        return agent.iterator();
    }

    /**
     * AND operator. a could be changed and returned.
     */
    public static BitMap and(BitMap a, BitMap b) {
        if (a == NONE || b == NONE) {
            return NONE;
        } else if (a == ALL) {
            return b;
        } else if (b == ALL) {
            return a;
        } else if (a == SOME || b == SOME) {
            return SOME;
        }
        a.agent.and(b.agent);
        return a;
    }

    /**
     * OR operator. a could be changed and returned.
     */
    public static BitMap or(BitMap a, BitMap b) {
        if (a == ALL || b == ALL) {
            return ALL;
        } else if (a == NONE) {
            return b;
        } else if (b == NONE) {
            return a;
        } else if (a == SOME || b == SOME) {
            return SOME;
        }
        a.agent.or(b.agent);
        return a;
    }

    /**
     * NOT operator. a could be changed and returned.
     */
    public static BitMap not(BitMap a) {
        if (a == ALL) {
            return NONE;
        } else if (a == NONE) {
            return ALL;
        } else if (a == SOME) {
            return SOME;
        }
        a.agent.flip(0, 65536);
        return a;
    }
}
