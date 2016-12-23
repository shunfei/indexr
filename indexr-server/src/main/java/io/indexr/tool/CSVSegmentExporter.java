package io.indexr.tool;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import io.indexr.io.ByteBufferReader;
import io.indexr.segment.ColumnSchema;
import io.indexr.segment.ColumnType;
import io.indexr.segment.Row;
import io.indexr.segment.Segment;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.pack.Integrated;
import io.indexr.segment.pack.IntegratedSegment;
import io.indexr.util.RuntimeUtil;

public class CSVSegmentExporter {
    private static final FileSystem fileSystem = FileSystems.getDefault();

    private final Segment segment;
    private final ColumnSchema[] columnSchemas;
    private final int[] valIndexes;
    private final String outPath;
    private final String spliter;

    private boolean stop;
    private long rowCount;

    public CSVSegmentExporter(Segment segment, List<String> csvColNames, String outPath, String spliter) throws IOException {
        this.segment = segment;
        this.outPath = outPath;
        this.spliter = spliter;
        SegmentSchema schema = segment.schema();
        this.columnSchemas = new ColumnSchema[schema.columns.size()];
        for (int colId = 0; colId < columnSchemas.length; colId++) {
            columnSchemas[colId] = schema.columns.get(colId);
        }

        if (csvColNames == null) {
            this.valIndexes = new int[columnSchemas.length];
            for (int i = 0; i < valIndexes.length; i++) {
                valIndexes[i] = i;
            }
        } else {
            this.valIndexes = new int[csvColNames.size()];
            for (int csvColId = 0; csvColId < valIndexes.length; csvColId++) {
                int valIndex = -1;
                String colName = csvColNames.get(csvColId);
                for (int colId = 0; colId < columnSchemas.length; colId++) {
                    if (columnSchemas[colId].name.equalsIgnoreCase(colName)) {
                        valIndex = colId;
                        break;
                    }
                }
                if (valIndex == -1) {
                    throw new IllegalStateException("Illegal column name: " + colName);
                }
                valIndexes[csvColId] = valIndex;
            }
        }

        if (outPath != null) {
            Path csvParent = fileSystem.getPath(outPath).getParent();
            if (csvParent != null) {
                Files.createDirectories(csvParent);
            }
        }
    }

    public long rowCount() {
        return rowCount;
    }

    public void start() throws FileNotFoundException, IOException {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                outPath == null ? System.out : new FileOutputStream(outPath, false), Charset.forName("UTF-8")))) {
            Iterator<Row> rows = segment.rowTraversal().iterator();
            StringBuilder builder = new StringBuilder(1024);
            while (rows.hasNext() && !stop) {
                builder.setLength(0);
                Row row = rows.next();
                for (int csvColId = 0; csvColId < valIndexes.length; csvColId++) {
                    int colId = valIndexes[csvColId];
                    ColumnSchema cs = columnSchemas[colId];
                    switch (cs.dataType) {
                        case ColumnType.INT:
                            builder.append(row.getInt(colId));
                            break;
                        case ColumnType.LONG:
                            builder.append(row.getLong(colId));
                            break;
                        case ColumnType.FLOAT:
                            builder.append(row.getFloat(colId));
                            break;
                        case ColumnType.DOUBLE:
                            builder.append(row.getDouble(colId));
                            break;
                        case ColumnType.STRING:
                            builder.append(row.getString(colId).toString());
                            break;
                        default:
                            throw new IllegalStateException("Unsupported column type: " + cs.dataType);
                    }
                    if (csvColId != valIndexes.length - 1) {
                        builder.append(spliter);
                    } else {
                        builder.append('\n');
                    }
                }
                try {
                    writer.write(builder.toString());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                rowCount++;
            }
            writer.flush();
            writer.close();
        }
    }

    public void stop() {
        stop = true;
    }


    private static class MyOptions {
        @Option(name = "-h", usage = "print this help")
        boolean help;
        @Option(name = "-s", required = true, metaVar = "path", usage = "segment path")
        String segmentPath;
        @Option(name = "-fh", metaVar = "path", usage = "csv header file's path, same as schema if ommited")
        String headerPath;
        @Option(name = "-csv", metaVar = "path", usage = "destination csv file path. Print to std if not specified.")
        String ouputPath = null;
        @Option(name = "-p", metaVar = "str", usage = "csv spliter")
        String spliter = ",";
        @Option(name = "-q", aliases = "quiet", usage = "print message or not")
        boolean quiet = false;
    }

    public static void main(String[] args) throws Exception {
        MyOptions options = new MyOptions();
        CmdLineParser parser = RuntimeUtil.parseArgs(args, options);
        if (options.help) {
            parser.printUsage(System.out);
            return;
        }
        List<String> csvColNames = null;
        if (options.headerPath != null) {
            csvColNames = Arrays.asList(IOUtils.toString(new FileInputStream(options.headerPath), "UTF-8").split(options.spliter));
        }

        URI uri = URI.create(options.segmentPath);
        Configuration fsConfig = new Configuration();
        fsConfig.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        org.apache.hadoop.fs.FileSystem fileSystem = org.apache.hadoop.fs.FileSystem.get(uri, fsConfig);

        org.apache.hadoop.fs.Path segmentPath = new org.apache.hadoop.fs.Path(options.segmentPath);
        FileStatus fileStatus = fileSystem.getFileStatus(segmentPath);

        ByteBufferReader.Opener readerOpener = ByteBufferReader.Opener.create(
                fileSystem,
                segmentPath,
                fileStatus.getLen());
        Integrated.SectionInfo sectionInfo = null;
        try (ByteBufferReader reader = readerOpener.open(0)) {
            sectionInfo = Integrated.read(reader);
            if (sectionInfo == null) {
                // Not a segment.
                System.err.printf("[%s] is not a valid segment", options.segmentPath);
                System.exit(1);
            }
            IntegratedSegment.Fd fd = IntegratedSegment.Fd.create("tmp_seg_name", sectionInfo, readerOpener);
            Segment segment = fd.open();
            CSVSegmentExporter exporter = new CSVSegmentExporter(segment, csvColNames, options.ouputPath, options.spliter);
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    exporter.stop();
                }
            });
            exporter.start();
            segment.close();
            if (!options.quiet) {
                System.out.printf("Successfully exported %d rows.\n", exporter.rowCount());
            }
        }
    }
}
