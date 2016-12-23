package io.indexr.segment.io;

import org.junit.Assert;
import org.junit.Test;

import io.indexr.io.ByteSlice;

public class FixedWidthNumbersTest {
    @Test
    public void testInts() {
        int[] arr = new int[]{99, 2, 51, 2, 5, 33, 11, 55, 1023, 222, 11, 65};
        int[] arr_overrited = new int[]{1023, 1023, 1023, 1023, 1023, 1023, 1023, 1023, 1023, 1023, 1023, 1023, 1023, 1023};

        ByteSlice source = ByteSlice.allocateHeap(1000);
        FixedWidthNumbers ints = FixedWidthNumbers.wrap(source, FixedWidthNumbers.calWidth(1023));
        //Assert.assertEquals(10, ints.width());

        for (int i = 0; i < arr_overrited.length; i++) {
            ints.set(i, arr_overrited[i]);
        }
        for (int i = 0; i < arr.length; i++) {
            ints.set(i, arr[i]);
        }

        int[] arr2 = new int[arr.length];
        for (int i = 0; i < arr.length; i++) {
            arr2[i] = (int) ints.get(i);
        }

        Assert.assertArrayEquals(arr, arr2);
    }

    @Test
    public void testIntsZero() {
        int[] arr = new int[]{0, 0, 0, 0, 0};

        ByteSlice source = ByteSlice.allocateHeap(1000);
        FixedWidthNumbers ints = FixedWidthNumbers.wrap(source, FixedWidthNumbers.calWidth(0));
        Assert.assertEquals(0, ints.width());

        for (int i = 0; i < arr.length; i++) {
            ints.set(i, arr[i]);
        }

        int[] arr2 = new int[arr.length];
        for (int i = 0; i < arr.length; i++) {
            arr2[i] = (int) ints.get(i);
        }

        Assert.assertArrayEquals(arr, arr2);
    }

    @Test
    public void testLongs() {
        long[] arr = new long[]{99, 2, 51, 2, 5, 33, 111111999991222222L, 55, 1023, 222, 11, 65};
        long[] arr_overrited = new long[]{11999991222222L, 11999991222222L, 11999991222222L, 11999991222222L, 11999991222222L,
                11999991222222L, 11999991222222L, 11999991222222L, 11999991222222L, 11999991222222L, 11999991222222L,
                11999991222222L, 11999991222222L, 11999991222222L};

        ByteSlice source = ByteSlice.allocateHeap(1000);
        FixedWidthNumbers longs = FixedWidthNumbers.wrap(source, FixedWidthNumbers.calWidth(111111999991222222L));
        //Assert.assertEquals(57, longs.width());

        for (int i = 0; i < arr_overrited.length; i++) {
            longs.set(i, arr_overrited[i]);
        }
        for (int i = 0; i < arr.length; i++) {
            longs.set(i, arr[i]);
        }

        long[] arr2 = new long[arr.length];
        for (int i = 0; i < arr.length; i++) {
            arr2[i] = longs.get(i);
        }

        Assert.assertArrayEquals(arr, arr2);
    }

    @Test
    public void testLongsWithGap() {
        long[] arr1 = new long[]{99, 2, 51, 2, 5, 33, 111111999991222222L, 55, 1023, 222, 11, 65};
        long[] arr2 = new long[]{666, 222, 777, 244, 8898, 3444, 8899, 2344, 55555, 221, 89, 3, 2, 3587848, 0};

        long[] arr_overrited = new long[]{11999991222222L, 11999991222222L, 11999991222222L, 11999991222222L, 11999991222222L,
                11999991222222L, 11999991222222L, 11999991222222L, 11999991222222L, 11999991222222L, 11999991222222L,
                11999991222222L, 11999991222222L, 11999991222222L};

        ByteSlice source = ByteSlice.allocateHeap(200);
        int width1 = FixedWidthNumbers.calWidth(111111999991222222L);
        //Assert.assertEquals(57, width1);
        int width2 = FixedWidthNumbers.calWidth(3587848);

        FixedWidthNumbers numbers_overide = FixedWidthNumbers.wrap(source, width1);
        FixedWidthNumbers numbers1 = FixedWidthNumbers.wrap(source, width1);
        FixedWidthNumbers numbers2 = FixedWidthNumbers.wrap(source.sliceFrom(FixedWidthNumbers.calByteSize(arr1.length, width1)), width2);

        for (int i = 0; i < arr_overrited.length; i++) {
            numbers_overide.set(i, arr_overrited[i]);
        }

        for (int i = 0; i < arr1.length; i++) {
            numbers1.set(i, arr1[i]);
        }
        for (int i = 0; i < arr2.length; i++) {
            numbers2.set(i, arr2[i]);
        }

        long[] arr_new1 = new long[arr1.length];
        for (int i = 0; i < arr1.length; i++) {
            arr_new1[i] = numbers1.get(i);
        }

        long[] arr_new2 = new long[arr2.length];
        for (int i = 0; i < arr2.length; i++) {
            arr_new2[i] = numbers2.get(i);
        }

        Assert.assertArrayEquals(arr1, arr_new1);
        Assert.assertArrayEquals(arr2, arr_new2);
    }
}
