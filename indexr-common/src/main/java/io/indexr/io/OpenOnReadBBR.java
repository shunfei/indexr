package io.indexr.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class OpenOnReadBBR implements ByteBufferReader {
    private static final Logger logger = LoggerFactory.getLogger(OpenOnReadBBR.class);

    private ByteBufferReader agent;
    private ByteBufferReader.Opener opener;
    private long readBase;

    public OpenOnReadBBR(ByteBufferReader.Opener opener, long readBase) {
        this.opener = opener;
    }

    private void checkOpen() throws IOException {
        if (agent == null) {
            agent = opener.open(readBase);
            if (logger.isDebugEnabled()) {
                logger.debug("late open");
            }
        }
    }

    @Override
    public void read(long position, ByteBuffer dst) throws IOException {
        checkOpen();
        agent.read(position, dst);
    }

    @Override
    public void read(long position, byte[] buffer, int offset, int length) throws IOException {
        checkOpen();
        agent.read(position, buffer, offset, length);
    }

    @Override
    public long readLong(long position) throws IOException {
        checkOpen();
        return agent.readLong(position);
    }

    @Override
    public boolean exists(long position) throws IOException {
        checkOpen();
        return exists(position);
    }

    @Override
    public void close() throws IOException {
        if (agent != null) {
            agent.close();
            agent = null;
        }
    }

    @Override
    public void setName(String name) {}
}
