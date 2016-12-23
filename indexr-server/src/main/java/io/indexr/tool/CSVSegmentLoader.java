package io.indexr.tool;

import com.google.common.base.Preconditions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import io.indexr.segment.ColumnSchema;
import io.indexr.segment.ColumnType;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.helper.SimpleRow;
import io.indexr.segment.pack.DPSegment;
import io.indexr.segment.pack.OpenOption;
import io.indexr.server.IndexRConfig;
import io.indexr.server.SegmentHelper;
import io.indexr.util.IOUtil;
import io.indexr.util.JsonUtil;
import io.indexr.util.RuntimeUtil;

/**
 * TODO handle the case when row number is larger then {@link io.indexr.segment.pack.StorageSegment#MAX_ROW_COUNT}.
 */
public class CSVSegmentLoader {
    private final SegmentSchema schema;
    private final ColumnSchema[] columnSchemas;
    private final int[] valIndexes;
    private final List<String> csvPaths;
    private final String spliter;

    private boolean interrupted;
    private long rowCount;

    private DPSegment segment;

    public CSVSegmentLoader(String name,
                            SegmentSchema schema,
                            List<String> csvColNames,
                            List<String> csvPaths,
                            Path outPath,
                            String spliter,
                            boolean compress) throws IOException {
        this.schema = schema;
        this.spliter = spliter;
        this.csvPaths = csvPaths;
        this.columnSchemas = new ColumnSchema[schema.columns.size()];
        this.valIndexes = new int[schema.columns.size()];
        for (int colId = 0; colId < schema.columns.size(); colId++) {
            ColumnSchema cs = schema.columns.get(colId);
            int valIndex = -1;
            if (csvColNames == null) {
                valIndex = colId;
            } else {
                for (int i = 0; i < csvColNames.size(); i++) {
                    if (cs.name.equalsIgnoreCase(csvColNames.get(i))) {
                        valIndex = i;
                        break;
                    }
                }
            }
            valIndexes[colId] = valIndex;
            columnSchemas[colId] = cs;
        }

        OpenOption[] modes = new OpenOption[]{OpenOption.Overwrite};
        this.segment = DPSegment.open(outPath.toString(), name, schema, modes).setCompress(compress);
    }

    public CSVSegmentLoader(String name, SegmentSchema schema, List<String> csvColNames, List<String> csvPaths, Path outPath) throws IOException {
        this(name, schema, csvColNames, csvPaths, outPath, ",", true);
    }

    public DPSegment segment() {
        return segment;
    }

    public void load() throws IOException {
        if (!csvPaths.isEmpty()) {
            segment.update();
            for (String file : csvPaths) {
                loadFile(segment, file);
            }
            segment.seal();
        }
    }

    private void loadFile(DPSegment segment, String path) throws IOException {
        List<Byte> types = schema.columns.stream().map(schema -> schema.dataType).collect(Collectors.toList());
        SimpleRow.Builder rowBuilder = new SimpleRow.Builder(types);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"))) {
            String line;
            while (StringUtils.isNotEmpty(line = reader.readLine()) && !interrupted) {
                String[] vals = line.split(spliter);
                try {
                    for (int colId = 0; colId < schema.columns.size(); colId++) {
                        ColumnSchema cs = columnSchemas[colId];
                        int valIndex = valIndexes[colId];
                        switch (cs.dataType) {
                            case ColumnType.INT:
                                if (valIndex == -1 || vals[valIndex].isEmpty()) {
                                    rowBuilder.appendInt(0);
                                } else {
                                    rowBuilder.appendInt(Integer.parseInt(vals[valIndex]));
                                }
                                break;
                            case ColumnType.LONG:
                                if (valIndex == -1 || vals[valIndex].isEmpty()) {
                                    rowBuilder.appendLong(0);
                                } else {
                                    rowBuilder.appendLong(Long.parseLong(vals[valIndex]));
                                }
                                break;
                            case ColumnType.FLOAT:
                                if (valIndex == -1 || vals[valIndex].isEmpty()) {
                                    rowBuilder.appendFloat(0);
                                } else {
                                    rowBuilder.appendFloat(Float.parseFloat(vals[valIndex]));
                                }
                                break;
                            case ColumnType.DOUBLE:
                                if (valIndex == -1 || vals[valIndex].isEmpty()) {
                                    rowBuilder.appendDouble(0);
                                } else {
                                    rowBuilder.appendDouble(Double.parseDouble(vals[valIndex]));
                                }
                                break;
                            case ColumnType.STRING:
                                if (valIndex == -1) {
                                    rowBuilder.appendString("");
                                } else {
                                    if (vals.length <= valIndex) {
                                        rowBuilder.appendString("");
                                        continue;
                                    }
                                    rowBuilder.appendString(vals[valIndex]);
                                }
                                break;
                            default:
                                throw new IllegalStateException("Unsupported column type: " + cs.dataType);
                        }
                    }
                } catch (NumberFormatException e) {
                    System.out.printf("error csv line: [%s]", line);
                    e.printStackTrace();
                }
                segment.add(rowBuilder.buildAndReset());
                rowCount++;
            }
        }
    }

    public long rowCount() {
        return rowCount;
    }

    public void stop() {
        interrupted = true;
    }

    private static class MyOptions {
        @Option(name = "-h", usage = "print this help")
        boolean help;
        @Option(name = "-ncp", usage = "if set, will not compress segment. Default is compressed.")
        boolean notcompress = false;
        @Option(name = "-q", aliases = "quiet", usage = "print message or not")
        boolean quiet = false;
        @Option(name = "-c", metaVar = "path", usage = "schema file")
        String schemaPath;
        @Option(name = "-fh", metaVar = "path", usage = "csv header file's path, same as schema if ommited")
        String headerPath;
        @Option(name = "-csv", metaVar = "path", usage = "csv files' path, support wildcard")
        String csvPath;
        @Option(name = "-s", metaVar = "path", usage = "output segment path")
        String ouputPath;
        @Option(name = "-tmp", metaVar = "path", usage = "temp path")
        String tmpPath = ".seg_gen_tmp." + UUID.randomUUID().toString();
        @Option(name = "-p", metaVar = "spliter", usage = "spliter")
        String spliter = ",";
    }

    public static void main(String[] args) {
        MyOptions options = new MyOptions();
        CmdLineParser parser = RuntimeUtil.parseArgs(args, options);
        if (options.help) {
            parser.printUsage(System.out);
            return;
        }

        Path tmpPath = Paths.get(options.tmpPath);
        IndexRConfig config = new IndexRConfig();
        int exitCode = 0;
        try {
            doLoad(config, options, options.ouputPath, tmpPath);
        } catch (Exception e) {
            System.err.printf("Load [%s] failed:\n", options.ouputPath);
            e.printStackTrace(System.err);
            exitCode = 1;
        } finally {
            IOUtils.closeQuietly(config);
            try {
                // Remove temporary dir.
                FileUtils.deleteDirectory(tmpPath.toFile());
                if (!options.quiet) {
                    System.out.printf("Deleted tmp path %s\n", tmpPath.toString());
                }
            } catch (IOException e) {
                System.err.println("Delete tmp path failed!");
                e.printStackTrace(System.err);
            }
        }
        System.exit(exitCode);
    }

    public static class MySchema {
        @JsonProperty("schema")
        public final SegmentSchema schema;

        @JsonCreator
        public MySchema(@JsonProperty("schema") SegmentSchema schema) {
            this.schema = schema;
        }
    }

    private static void doLoad(IndexRConfig config, MyOptions options, String outputPath, Path tmpPath) throws Exception {
        MySchema schema = options.schemaPath == null ? null : JsonUtil.load(Paths.get(options.schemaPath), MySchema.class);
        Preconditions.checkState(options.schemaPath != null, "Please specify schema path");
        Preconditions.checkState(schema.schema != null, "Illegal schame");

        List<String> csvColNames = null;
        if (options.headerPath != null) {
            csvColNames = Arrays.asList(IOUtils.toString(new FileInputStream(options.headerPath), "UTF-8").split(options.spliter));
        }
        List<String> csvPaths = options.csvPath == null ? Collections.emptyList() : IOUtil.wildcardFiles(options.csvPath);

        CSVSegmentLoader loader = new CSVSegmentLoader(
                "tmp_gen_name",
                schema.schema,
                csvColNames,
                csvPaths,
                tmpPath,
                options.spliter,
                !options.notcompress);

        DPSegment segment = loader.segment();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                loader.stop();
            }
        });

        loader.load();
        if (!options.quiet) {
            System.out.printf("Loaded [%d] rows from csv files: %s\n", loader.rowCount(), csvPaths);
        }

        if (loader.interrupted) {
            return;
        }

        URI uri = URI.create(outputPath);
        Configuration fsConfig = new Configuration();
        fsConfig.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        FileSystem fileSystem = FileSystem.get(uri, fsConfig);

        org.apache.hadoop.fs.Path realPath = new org.apache.hadoop.fs.Path(outputPath);
        if (fileSystem.exists(realPath)) {
            if (!options.quiet) {
                System.out.printf("Found file at [%s], remove it first...\n", fileSystem.resolvePath(realPath));
            }
            fileSystem.delete(realPath, false);
        }
        SegmentHelper.uploadSegment(segment, fileSystem, realPath, false);
        if (!options.quiet) {
            System.out.printf("Generated segment [%s]\n", fileSystem.resolvePath(realPath));
        }
    }
}
