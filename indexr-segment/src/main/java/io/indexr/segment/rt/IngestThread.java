package io.indexr.segment.rt;

public class IngestThread extends Thread {

    private volatile boolean interrupValid = false;

    public IngestThread(Runnable target, String tableName) {
        super(target, "RT-Ingest-" + tableName);
    }

    public boolean isInterrupValid() {
        return interrupValid;
    }

    public void setInterrupValid(boolean v) {
        this.interrupValid = v;
    }
}
