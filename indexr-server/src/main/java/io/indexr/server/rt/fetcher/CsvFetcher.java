package io.indexr.server.rt.fetcher;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.directory.api.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

import io.indexr.segment.SQLType;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.rt.Fetcher;
import io.indexr.segment.rt.UTF8JsonRowCreator;
import io.indexr.segment.rt.UTF8Row;
import io.indexr.util.Trick;
import io.indexr.util.Try;

// This class should be refactored.
public class CsvFetcher implements Fetcher {
    private static final Logger logger = LoggerFactory.getLogger(CsvFetcher.class);

    @JsonProperty("path")
    public final String path;
    @JsonProperty("header")
    public String[] header;
    @JsonProperty("spliter")
    public final String spliter;

    private SQLType[] colTypes;
    private BufferedReader reader;
    private String line;

    private final UTF8JsonRowCreator utf8JsonRowCreator = new UTF8JsonRowCreator();

    public CsvFetcher(@JsonProperty("path") String path,
                      @JsonProperty("header") List<String> header,
                      @JsonProperty("spliter") String spliter) {
        this.path = path;
        this.header = header == null ? null : header.toArray(new String[header.size()]);
        this.spliter = spliter == null ? "," : spliter;
    }

    @Override
    public void setRowCreator(String name, UTF8Row.Creator rowCreator) {
        utf8JsonRowCreator.setRowCreator(name, rowCreator);
    }

    @Override
    public boolean ensure(SegmentSchema schema) throws Exception {
        if (reader != null) {
            return true;
        }
        if (header == null) {
            header = new String[schema.columns.size()];
            colTypes = new SQLType[header.length];
            for (int colId = 0; colId < header.length; colId++) {
                header[colId] = schema.columns.get(colId).getName();
                colTypes[colId] = schema.columns.get(colId).getSqlType();
            }
        } else {
            colTypes = new SQLType[header.length];
            for (int colId = 0; colId < colTypes.length; colId++) {
                String name = header[colId];
                int index = Trick.indexWhere(schema.columns, cs -> cs.getName().equals(name));
                if (index >= 0) {
                    colTypes[colId] = schema.columns.get(index).getSqlType();
                }
            }
        }
        reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
        return true;
    }

    @Override
    public boolean hasNext() throws Exception {
        if (reader == null) {
            return false;
        }
        if (line != null) {
            return true;
        }
        line = reader.readLine();
        return line != null;
    }

    @Override
    public List<UTF8Row> next() throws Exception {
        String[] values = line.split(spliter);
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        for (int colId = 0; colId < header.length; colId++) {
            sb.append('\"').append(header[colId]).append("\": ");
            String v = values[colId];
            if (colTypes[colId] == SQLType.VARCHAR
                    || colTypes[colId] == SQLType.DATE
                    || colTypes[colId] == SQLType.TIME
                    || colTypes[colId] == SQLType.DATETIME) {
                sb.append('\"').append(v).append('\"');
            } else {
                if (Strings.isEmpty(v)) {
                    sb.append('0');
                } else {
                    sb.append(v);
                }
            }
            if (colId < (header.length - 1)) {
                sb.append(',');
            }
        }
        sb.append('}');

        line = null;

        byte[] data = sb.toString().getBytes("utf-8");
        return utf8JsonRowCreator.create(data);
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            Try.on(reader::close, logger);
            reader = null;
        }
    }

    @Override
    public void commit() {}

    @Override
    public boolean equals(Fetcher o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CsvFetcher that = (CsvFetcher) o;

        if (path != null ? !path.equals(that.path) : that.path != null) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(header, that.header)) return false;
        return spliter != null ? spliter.equals(that.spliter) : that.spliter == null;

    }

    @Override
    public String toString() {
        return "CsvFetcher{" +
                "path='" + path + '\'' +
                ", header=" + Arrays.toString(header) +
                ", spliter='" + spliter + '\'' +
                '}';
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
}
