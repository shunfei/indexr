package io.indexr.segment.helper;

import com.google.common.base.Preconditions;

import org.apache.directory.api.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.indexr.segment.RSValue;
import io.indexr.segment.Segment;
import io.indexr.segment.SegmentFd;
import io.indexr.segment.SegmentPool;
import io.indexr.segment.SystemConfig;
import io.indexr.segment.pack.DataPack;
import io.indexr.segment.pack.ExtIndexMemCache;
import io.indexr.segment.pack.IndexMemCache;
import io.indexr.segment.pack.PackMemCache;
import io.indexr.segment.rc.RCOperator;
import io.indexr.segment.rt.RTSGroup;
import io.indexr.segment.rt.RTSGroupInfo;
import io.indexr.segment.rt.RealtimeSegment;

public class SegmentAssigner {
    private static final int packSplit = 8;
    private static final long chunkRowCount = DataPack.MAX_COUNT / packSplit;
    private static final Logger log = LoggerFactory.getLogger(SegmentAssigner.class);

    // not columned < columned, bigger row count < less row count.
    private static Comparator<Segment> segmentCmp = new Comparator<Segment>() {
        @Override
        public int compare(Segment s1, Segment s2) {
            if (!s1.isColumned() && s2.isColumned()) {
                return -1;
            } else if (s1.isColumned() && !s2.isColumned()) {
                return 1;
            } else {
                long l = s1.rowCount() - s2.rowCount();
                if (l == 0) {
                    return 0;
                } else {
                    return l > 0 ? -1 : 1;
                }
            }
        }
    };

    private static void doAssign(Map<Integer, List<SingleWork>> assignMap, int id, SingleWork assignment) {
        List<SingleWork> list = assignMap.get(id);
        if (list == null) {
            list = new ArrayList<>();
        }
        list.add(assignment);
        assignMap.put(id, list);
    }

    public static Map<Integer, List<SingleWork>> assignBalance(String myHost,
                                                               int assignCount,
                                                               List<RangeWork> works,
                                                               RCOperator rsFilter,
                                                               SegmentPool segmentPool,
                                                               IndexMemCache indexMemCache,
                                                               ExtIndexMemCache extIndexMemCache,
                                                               PackMemCache packMemCache) throws Exception {
        log.debug("to assign assignCount:{}, works:{}, rsFilter:{}", assignCount, works, rsFilter);

        works = RangeWork.compact(works);

        // approximate count.
        long totalRowCount = 0;
        long validRowCount = 0;

        List<SingleWork> validWorks = new ArrayList<>(2048);
        boolean refreshed = false;
        for (RangeWork work : works) {
            SegmentFd fd = segmentPool.get(work.segment());
            if (!refreshed) {

                // Realtime segment only exists on its generating local node.
                // It will be uploaded to file system and then becomes a new history segment.
                // But other hosts (like the node running this code) may have some delay on info update.
                // We have to manually update the infomation.

                if (fd == null || (fd instanceof RTSGroupInfo && !Strings.equals(((RTSGroupInfo) fd).host(), myHost))) {
                    segmentPool.refresh(false);
                    refreshed = true;
                    fd = segmentPool.get(work.segment());
                }
            }
            if (fd == null) {
                log.warn("Segment not found! [segment: {}]", work.segment());
                if (SystemConfig.FAILFAST.getBool()) {
                    throw new IllegalStateException(String.format("Segment not found! [segment: %s]", work.segment()));
                }
                continue;
            }

            long segmentRowCount = fd.info().rowCount();

            if (fd instanceof RTSGroupInfo) {
                Preconditions.checkState(work.startPackId() == -1);
                totalRowCount += segmentRowCount;

                RTSGroupInfo rtsgInfo = (RTSGroupInfo) fd;
                RTSGroup rtsg = rtsgInfo.getRTSGroup();
                if (rtsg == null) {
                    if (Strings.equals(myHost, rtsgInfo.host())) {
                        throw new IllegalStateException("Forgot to set rtsg?");
                    } else {
                        throw new IllegalStateException("Being assigned a rtsg which not belong to this host!");
                    }
                }
                for (RealtimeSegment rts : rtsg.realtimeSegments().values()) {
                    try (Segment segment = rts.open(indexMemCache, extIndexMemCache, packMemCache)) {
                        if (rsFilter != null) {
                            rsFilter.materialize(segment.schema().getColumns());
                        }
                        if (segment.isColumned()) {
                            long rtsSegRowCount = segment.rowCount();
                            for (int packId = 0; packId < segment.packCount(); packId++) {
                                byte rsRes = RSValue.Some;
                                if (rsFilter == null || (rsRes = rsFilter.roughCheckOnPack(segment, packId)) != RSValue.None) {
                                    validWorks.add(new SingleWork(segment.name(), packId));
                                    validRowCount += DataPack.packRowCount(rtsSegRowCount, packId);
                                } else {
                                    log.debug("rs filter ignore segment {} pack {}", segment.name(), packId);
                                }
                            }
                        } else {
                            validWorks.add(new SingleWork(segment.name(), -1));
                            validRowCount += segment.rowCount();
                        }
                    }
                }
            } else {
                try (Segment segment = fd.open(indexMemCache, extIndexMemCache, packMemCache)) {
                    Preconditions.checkState(segment.isColumned());
                    if (rsFilter != null) {
                        rsFilter.materialize(segment.schema().getColumns());
                    }
                    int startPackId = work.startPackId();
                    int endPackId = work.endPackId();
                    if (startPackId == -1) {
                        // It is a realtime segment when planning, but transformed into historical segment now.
                        startPackId = 0;
                        endPackId = segment.packCount();
                    }
                    for (int packId = startPackId; packId < endPackId; packId++) {
                        int packRowCount = DataPack.packRowCount(segmentRowCount, packId);
                        totalRowCount += packRowCount;

                        byte rsRes = RSValue.Some;
                        if (rsFilter == null || (rsRes = rsFilter.roughCheckOnPack(segment, packId)) != RSValue.None) {
                            validWorks.add(new SingleWork(segment.name(), packId));
                            validRowCount += packRowCount;
                        } else {
                            log.debug("rs filter ignore segment {} pack {}", segment.name(), packId);
                        }
                    }
                }
            }
        }

        if (log.isInfoEnabled()) {
            double passRate = totalRowCount == 0 ? 0.0 : ((double) validRowCount) / totalRowCount;
            passRate = Math.min(passRate, 1.0);
            log.info("Pass rate: {}, scan row: {}", String.format("%.2f%%", (float) (passRate * 100)), validRowCount);
        }

        int workSize = validWorks.size();
        int avgWorkCount = workSize / assignCount;
        int plusOneWork = workSize % assignCount;

        Map<Integer, List<SingleWork>> assignMap = new HashMap<>(assignCount);
        int workOffset = 0;
        for (int assignIndex = 0; assignIndex < assignCount; assignIndex++) {
            int count = assignIndex <= (plusOneWork - 1) ? avgWorkCount + 1 : avgWorkCount;
            for (SingleWork ass : validWorks.subList(workOffset, workOffset + count)) {
                doAssign(assignMap, assignIndex, ass);
            }
            workOffset += count;
        }
        Preconditions.checkState(workOffset == workSize, "current workOffset: %s, workSize: %s", workOffset, workSize);

        if (log.isDebugEnabled()) {
            Map<Integer, List<RangeWork>> compactAssignMap = new HashMap<>(assignCount);
            for (Map.Entry<Integer, List<SingleWork>> e : assignMap.entrySet()) {
                compactAssignMap.put(e.getKey(), RangeWork.compact(e.getValue()));
            }
            log.debug("assignment: {}", compactAssignMap);
        }

        return assignMap;
    }
}
