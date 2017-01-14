package io.indexr.server.rt.fetcher;


import com.google.common.base.Preconditions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.directory.api.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.indexr.segment.SegmentSchema;
import io.indexr.segment.rt.Fetcher;
import io.indexr.segment.rt.UTF8JsonRowCreator;
import io.indexr.segment.rt.UTF8Row;
import io.indexr.util.CombinedBlockingIterator;
import io.indexr.util.IOUtil;
import io.indexr.util.JsonUtil;
import io.indexr.util.UTF8Util;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class Kafka08Fetcher implements Fetcher {
    private static final Logger logger = LoggerFactory.getLogger(Kafka08Fetcher.class);

    @JsonProperty("topic")
    public final String topic;
    @JsonProperty("topics")
    public final List<String> topics;
    @JsonProperty("number.empty.as.zero")
    public boolean numberEmptyAsZero;
    @JsonProperty("properties")
    public final Properties properties;

    private ConsumerConnector connector;
    private Iterator<byte[]> eventItr;
    private volatile List<byte[]> remaining;

    private final UTF8JsonRowCreator utf8JsonRowCreator;

    @JsonCreator
    public Kafka08Fetcher(@JsonProperty("topic") String topic,
                          @JsonProperty("topics") List<String> topics,
                          @JsonProperty("number.empty.as.zero") Boolean numberEmptyAsZero,
                          @JsonProperty("properties") Properties properties) {
        if (topics == null || topics.isEmpty()) {
            Preconditions.checkState(!Strings.isEmpty(topic), "topic or topics should be specified!");
            topics = Collections.singletonList(topic);
        }
        this.topic = topic;
        this.topics = topics;
        this.numberEmptyAsZero = numberEmptyAsZero == null ? false : numberEmptyAsZero;
        this.properties = properties;

        this.utf8JsonRowCreator = new UTF8JsonRowCreator(this.numberEmptyAsZero);
    }

    @Override
    public void setRowCreator(String name, UTF8Row.Creator rowCreator) {
        utf8JsonRowCreator.setRowCreator(name, rowCreator);
    }

    @Override
    public boolean ensure(SegmentSchema schema) throws Exception {
        if (connector != null && eventItr != null) {
            try {
                return eventItr.hasNext();
            } catch (Exception e) {
                if (e instanceof InterruptedException) {
                    return false;
                }
                // Something bad happen, let's reopen it.
            }
        }
        logger.debug("Try open kafka fetcher. [topic: {}]", topics);

        synchronized (this) {
            // synchronized code block cannot contains "consumerIterator.hasNext()"
            // because it waits until at least one message come in.
            if (connector != null) {
                try {
                    connector.shutdown();
                } catch (Exception e) {
                    logger.error("Shutdown connector failed", e);
                }
                connector = null;
            }
            IOUtil.closeQuietly(eventItr);
            eventItr = null;

            connector = Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
            Map<String, Integer> tcm = new HashMap<>();
            for (String topic : topics) {
                tcm.put(topic, 1);
            }
            logger.debug("topics: {}", topics);
            Map<String, List<KafkaStream<byte[], byte[]>>> streams = connector.createMessageStreams(tcm);
            List<Iterator<byte[]>> iterators = new ArrayList<>();
            for (String topic : topics) {
                List<KafkaStream<byte[], byte[]>> streamList = streams.get(topic);
                Preconditions.checkState(streamList != null, "Create kafka stream list failed");
                ConsumerIterator<byte[], byte[]> it = streamList.get(0).iterator();
                iterators.add(toItr(it));
            }
            if (iterators.size() == 1) {
                eventItr = iterators.get(0);
            } else {
                eventItr = new CombinedBlockingIterator<>(iterators);
            }
        }

        return eventItr.hasNext();
    }

    @Override
    public boolean hasNext() throws Exception {
        if (this.remaining != null) {
            return true;
        }
        Iterator itr = eventItr;
        return itr != null && itr.hasNext();
    }

    @Override
    public List<UTF8Row> next() throws Exception {
        List<byte[]> remaining = this.remaining;
        this.remaining = null;
        if (remaining != null) {
            List<UTF8Row> rows = new ArrayList<>(remaining.size());
            for (byte[] data : remaining) {
                rows.addAll(parseUTF8Row(data));
            }
            return rows;
        }

        Iterator<byte[]> itr = eventItr;
        if (itr == null) {
            return Collections.emptyList();
        }
        return parseUTF8Row(itr.next());
    }

    private List<UTF8Row> parseUTF8Row(byte[] data) {
        try {
            return utf8JsonRowCreator.create(data);
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Illegal data: {} ", UTF8Util.fromUtf8(data), e);
            }
            return Collections.emptyList();
        }
    }

    @Override
    public synchronized void close() throws IOException {
        logger.debug("Stop kafka fetcher. [topic: {}]", topics);
        ConsumerConnector connector = this.connector;
        this.connector = null;
        if (connector != null) {
            connector.commitOffsets();
            connector.shutdown();
        }

        IOUtil.closeQuietly(eventItr);
        // Some events could exists in the buffer, try to save them.
        List<byte[]> remaining = new ArrayList<>();
        try {
            while (eventItr.hasNext()) {
                remaining.add(eventItr.next());
            }
        } catch (Exception e) {
            // Ignore
        }
        eventItr = null;
        if (!remaining.isEmpty()) {
            this.remaining = remaining;
        }
    }

    @Override
    public void commit() {
        ConsumerConnector connector = this.connector;
        if (connector != null) {
            connector.commitOffsets();
        }
    }

    @Override
    public String toString() {
        String settings = JsonUtil.toJson(this);
        return String.format("Kafka 0.8 fetcher: %s", settings);
    }

    @Override
    public boolean equals(Fetcher o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Kafka08Fetcher that = (Kafka08Fetcher) o;

        if (topic != null ? !topic.equals(that.topic) : that.topic != null) return false;
        if (topics != null ? !topics.equals(that.topics) : that.topics != null) return false;
        return properties != null ? properties.equals(that.properties) : that.properties == null;

    }

    @Override
    public long statConsume() {
        return utf8JsonRowCreator.getConsumeCount();
    }

    @Override
    public long statProduce() {
        return utf8JsonRowCreator.getProduceCount();
    }

    @Override
    public long statIgnore() {
        return utf8JsonRowCreator.getIgnoreCount();
    }

    @Override
    public long statFail() {
        return utf8JsonRowCreator.getFailCount();
    }

    @Override
    public void statReset() {
        utf8JsonRowCreator.resetStat();
    }

    private Iterator<byte[]> toItr(ConsumerIterator<byte[], byte[]> consumerIterator) {
        return new Iterator<byte[]>() {
            @Override
            public boolean hasNext() {
                return consumerIterator.hasNext();
            }

            @Override
            public byte[] next() {
                return consumerIterator.next().message();
            }
        };
    }
}
