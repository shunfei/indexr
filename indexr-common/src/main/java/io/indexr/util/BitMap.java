package io.indexr.util;

import io.indexr.data.Freeable;

/**
 * A wrapper of the underlying bitmap.
 *
 * We guarantee that the content of {@link #ALL} or {@link #NONE} will never be changed.
 * As those static instances are shared around.
 */
public class BitMap implements Freeable {
    public static final int DATAPACK_MAX_COUNT = 65536;

    //public static final BitMap SOME = new BitMap(null);
    public static final BitMap ALL = new BitMap(null, 0);
    public static final BitMap NONE = new BitMap(null, 0);

    public DirectBitMap agent;
    private int bits;

    public BitMap() {
        this(DATAPACK_MAX_COUNT);
    }

    public BitMap(int bits) {
        agent = new DirectBitMap(bits);
        this.bits = bits;
    }

    public BitMap(DirectBitMap agent, int bits) {
        this.agent = agent;
        this.bits = bits;
    }

    @Override
    public void free() {
        //if (this == SOME || this == ALL || this == NONE) {
        if (this == ALL || this == NONE) {
            // Those can not change.
            return;
        }
        if (agent != null) {
            agent.free();
            agent = null;
        }
    }

    @Override
    public String toString() {
        //if (this == SOME) {
        //    return "SOME";
        //} else
        if (this == ALL) {
            return "ALL";
        } else if (this == NONE) {
            return "NONE";
        } else {
            String s = agent.toString();
            return s.substring(0, Math.min(s.length(), 8));
        }
    }

    public void set(int id) {
        //assert this != SOME;
        assert this != ALL;
        assert this != NONE;

        agent.set(id);
    }

    public void unset(int start, int end) {
        //assert this != SOME;
        assert this != ALL;
        assert this != NONE;

        agent.clear(start, end);
    }

    public boolean get(int id) {
        if (this == ALL) {
            return true;
        } else if (this == NONE) {
            return false;
        }
        return agent.get(id);
    }

    //public int cardinality() {
    //    return (int) agent.cardinality();
    //}

    public boolean isEmpty() {
        if (this == ALL) {
            return false;
        } else if (this == NONE) {
            return true;
        }
        int next = agent.nextSetBit(0);
        return next == -1 || next >= bits;
    }

    public DirectBitMapIterator iterator() {
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
            //} else if (a == SOME || b == SOME) {
            //    return SOME;
        }
        a.agent.and(b.agent);
        return a;
    }

    /**
     * AND operator. a could be changed and returned.
     * And free the unused bitmap.
     */
    public static BitMap and_free(BitMap a, BitMap b) {
        if (a == NONE) {
            b.free();
            return NONE;
        } else if (b == NONE) {
            a.free();
            return NONE;
        } else if (a == ALL) {
            return b;
        } else if (b == ALL) {
            return a;
            //} else if (a == SOME) {
            //    b.free();
            //    return SOME;
            //} else if (b == SOME) {
            //    a.free();
            //    return SOME;
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
            //} else if (a == SOME || b == SOME) {
            //    return SOME;
        }
        a.agent.or(b.agent);
        return a;
    }

    /**
     * OR operator. a could be changed and returned.
     * And free the unused bitmap.
     */
    public static BitMap or_free(BitMap a, BitMap b) {
        if (a == ALL) {
            b.free();
            return ALL;
        } else if (b == ALL) {
            a.free();
            return ALL;
        } else if (a == NONE) {
            return b;
        } else if (b == NONE) {
            return a;
            //} else if (a == SOME) {
            //    b.free();
            //    return SOME;
            //} else if (b == SOME) {
            //    a.free();
            //    return SOME;
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
            //} else if (a == SOME) {
            //    return SOME;
        }
        a.agent.flip(0, a.agent.size());
        return a;
    }
}
