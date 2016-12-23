package io.indexr.segment.pack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class PackDurationStat {
    private static final Logger logger = LoggerFactory.getLogger(PackDurationStat.class);
    public static final PackDurationStat INSTANCE = new PackDurationStat();
    private static PackDurationStat LAST = INSTANCE;

    // ms
    public final AtomicLong loadDPN = new AtomicLong();
    public final AtomicLong loadIndex = new AtomicLong();
    public final AtomicLong loadPack = new AtomicLong();
    public final AtomicLong compressPack = new AtomicLong();
    public final AtomicLong decompressPack = new AtomicLong();

    private PackDurationStat() {}

    @Override
    protected PackDurationStat clone() {
        PackDurationStat stat = new PackDurationStat();
        stat.loadDPN.set(loadDPN.get());
        stat.loadIndex.set(loadIndex.get());
        stat.loadPack.set(loadPack.get());
        stat.compressPack.set(compressPack.get());
        stat.decompressPack.set(decompressPack.get());
        return stat;
    }

    private PackDurationStat delta(PackDurationStat stat) {
        PackDurationStat delta = new PackDurationStat();
        delta.loadDPN.set(loadDPN.get() - stat.loadDPN.get());
        delta.loadIndex.set(loadIndex.get() - stat.loadIndex.get());
        delta.loadPack.set(loadPack.get() - stat.loadPack.get());
        delta.compressPack.set(compressPack.get() - stat.compressPack.get());
        delta.decompressPack.set(decompressPack.get() - stat.decompressPack.get());
        return delta;
    }

    void add_loadDPN(long time) {
        loadDPN.addAndGet(time);
    }

    void add_loadIndex(long time) {
        loadIndex.addAndGet(time);
    }

    void add_loadPack(long time) {
        loadPack.addAndGet(time);
    }

    void add_compressPack(long time) {
        compressPack.addAndGet(time);
    }

    void add_decompressPack(long time) {
        decompressPack.addAndGet(time);
    }

    @Override
    public String toString() {
        return String.format(
                "loadDPN: %ss, loadIndex: %ss, loadPack: %ss, compressPack: %ss, decompressPack: %ss, decompressPack / loadPack: %s",
                loadDPN.get() / 1000, loadIndex.get() / 1000, loadPack.get() / 1000, compressPack.get() / 1000, decompressPack.get() / 1000,
                String.format("%.2f", (double) (decompressPack.get()) / loadPack.get()));
    }

    public static void print() {
        PackDurationStat delta = INSTANCE.delta(LAST);
        LAST = INSTANCE.clone();
        logger.info("==================");
        logger.info("DurationStat - History Total: " + INSTANCE.toString());
        logger.info("DurationStat - Delta(60s): " + delta.toString());
        logger.info("==================");
    }
}
