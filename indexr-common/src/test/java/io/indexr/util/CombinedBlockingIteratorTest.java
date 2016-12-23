package io.indexr.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class CombinedBlockingIteratorTest {
    private static final Random random = new Random();

    @Test
    public void test() {
        List<Iterator<Integer>> itrs = new ArrayList<>();
        int itrCount = 10;
        int listSize = 10000;
        long sum = 0;
        for (int i = 0; i < itrCount; i++) {
            List<Integer> list = randomList(listSize);
            sum += list.stream().map(a -> (long) a).reduce(0L, Long::sum);
            itrs.add(list.iterator());
        }

        CombinedBlockingIterator<Integer> combinedBlockingIterator = new CombinedBlockingIterator<>(itrs);
        long newSum = 0;
        int itemCount = 0;
        while (combinedBlockingIterator.hasNext()) {
            newSum += combinedBlockingIterator.next();
            itemCount++;
        }
        Assert.assertEquals(itrCount * listSize, itemCount);
        Assert.assertEquals(sum, newSum);
    }

    private static List<Integer> randomList(int number) {
        List<Integer> list = new ArrayList<>(number);
        for (int i = 0; i < number; i++) {
            list.add(random.nextInt());
        }
        return list;
    }
}
