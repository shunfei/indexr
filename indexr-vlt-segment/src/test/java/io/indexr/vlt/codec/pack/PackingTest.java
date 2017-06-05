/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.indexr.vlt.codec.pack;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class PackingTest {
    private static final Logger LOG = LoggerFactory.getLogger(PackingTest.class);

    @Test
    public void testIntPackUnPack() {
        for (int i = 0; i < 100; i++) {
            testIntPackUnPack(i, false);
        }
    }

    @Test
    public void testIntPackUnPack2() {
        for (int i = 0; i < 100; i++) {
            testIntPackUnPack(i, true);
        }
    }

    private void testIntPackUnPack(int count, boolean b) {
        LOG.debug("");
        LOG.debug("testPackUnPack");
        for (int i = 1; i < 32; i++) {
            LOG.debug("Width: " + i);
            int[] unpacked = new int[count];
            int[] values = generateValues(i, count);
            intPackUnpack(PackingUtil.intPacker(i), values, unpacked, b);
            LOG.debug("Output: " + toString(unpacked));
            Assert.assertArrayEquals("width " + i, values, unpacked);
        }
    }


    @Test
    public void testLongPackUnPack2() {
        for (int i = 0; i < 100; i++) {
            testLongPackUnPack(i, true);
        }
    }

    @Test
    public void atestLongPackUnPack() {
        for (int i = 0; i < 100; i++) {
            testLongPackUnPack(i, false);
        }
    }


    private void testLongPackUnPack(int count, boolean b) {
        LOG.debug("");
        LOG.debug("testPackUnPackLong");
        for (int i = 1; i < 64; i++) {
            LOG.debug("Width: " + i);
            long[] unpacked8 = new long[count];
            long[] values = generateValuesLong(i, count);
            longPackUnpack(PackingUtil.longPacker(i), values, unpacked8, b);
            LOG.debug("Output 8: " + toString(unpacked8));
            Assert.assertArrayEquals("width " + i, values, unpacked8);
        }
    }

    private void intPackUnpack(IntPacker packer, int[] values, int[] unpacked, boolean b) {
        byte[] packed = new byte[PackingUtil.packingOutputBytes(values.length, packer.bitWidth())];
        if (b) {
            packer._pack(values, packed);
        } else {
            packer.pack(values, packed);
        }
        LOG.debug("packed: " + toString(packed));
        if (b) {
            packer._unpack(packed, unpacked);
        } else {
            packer.unpack(packed, unpacked);
        }
    }

    private void longPackUnpack(LongPacker packer, long[] values, long[] unpacked, boolean b) {
        byte[] packed = new byte[PackingUtil.packingOutputBytes(values.length, packer.bitWidth())];
        if (b) {
            packer._pack(values, packed);
        } else {
            packer.pack(values, packed);
        }
        LOG.debug("packed: " + toString(packed));
        if (b) {
            packer._unpack(packed, unpacked);
        } else {
            packer.unpack(packed, unpacked);
        }
    }

    private int[] generateValues(int bitWidth, int count) {
        int[] values = new int[count];
        for (int j = 0; j < values.length; j++) {
            values[j] = (int) (Math.random() * 100000) % (int) Math.pow(2, bitWidth);
        }
        LOG.debug("Input:  " + toString(values));
        return values;
    }

    private long[] generateValuesLong(int bitWidth, int count) {
        long[] values = new long[count];
        Random random = new Random(0);
        for (int j = 0; j < values.length; j++) {
            values[j] = random.nextLong() & ((1l << bitWidth) - 1l);
        }
        LOG.debug("Input:  " + toString(values));
        return values;
    }


    public static String toString(int[] vals) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (int i : vals) {
            if (first) {
                first = false;
            } else {
                sb.append(" ");
            }
            sb.append(i);
        }
        return sb.toString();
    }

    public static String toString(long[] vals) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (long i : vals) {
            if (first) {
                first = false;
            } else {
                sb.append(" ");
            }
            sb.append(i);
        }
        return sb.toString();
    }

    public static String toString(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (byte b : bytes) {
            if (first) {
                first = false;
            } else {
                sb.append(" ");
            }
            int i = b < 0 ? 256 + b : b;
            String binaryString = Integer.toBinaryString(i);
            for (int j = binaryString.length(); j < 8; ++j) {
                sb.append("0");
            }
            sb.append(binaryString);
        }
        return sb.toString();
    }
}
