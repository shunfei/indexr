package io.indexr.segment.helper;

import org.junit.Ignore;
import org.junit.Test;

import io.indexr.segment.Column;
import io.indexr.segment.RowTraversal;
import io.indexr.segment.Segment;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.pack.DataPack;

public class SegmentAssignerTest {

    private static class FakeRealtimeSegment implements Segment {
        @Override
        public String name() {
            return "realtime-fake";
        }

        @Override
        public SegmentSchema schema() {
            return null;
        }

        @Override
        public boolean isColumned() {
            return false;
        }

        @Override
        public int packCount() {
            return 0;
        }

        @Override
        public long rowCount() {
            return 1000000;
        }

        @Override
        public Column column(int colId) {
            return null;
        }

        @Override
        public RowTraversal rowTraversal(long offset, long count) {
            return null;
        }

        @Override
        public String toString() {
            return name();
        }
    }

    private static class FakeDPSegment extends FakeRealtimeSegment {
        @Override
        public String name() {
            return "dp-fake";
        }

        @Override
        public boolean isColumned() {
            return true;
        }

        @Override
        public int packCount() {
            return (int) (rowCount() / DataPack.MAX_COUNT + 1);
        }

        @Override
        public String toString() {
            return name();
        }
    }


    @Test
    @Ignore
    public void test() {
        //int assignCount = 100;
        //System.out.println(SegmentAssigner.assignBalance(assignCount, Lists.newArrayList(
        //        new FakeRealtimeSegment(),
        //        new FakeDPSegment(),
        //        new FakeDPSegment(),
        //        new FakeDPSegment(),
        //        new FakeRealtimeSegment()),
        //        null));
    }
}
