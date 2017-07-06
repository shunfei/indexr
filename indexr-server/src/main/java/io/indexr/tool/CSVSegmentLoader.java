package io.indexr.tool;

import com.google.common.base.Preconditions;

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

import io.indexr.plugin.Plugins;
import io.indexr.segment.ColumnSchema;
import io.indexr.segment.Row;
import io.indexr.segment.SegmentMode;
import io.indexr.segment.helper.SimpleRow;
import io.indexr.segment.pack.DataPack;
import io.indexr.segment.storage.DPSegment;
import io.indexr.segment.storage.OpenOption;
import io.indexr.segment.storage.SortedSegmentGenerator;
import io.indexr.segment.storage.StorageSegment;
import io.indexr.segment.storage.Version;
import io.indexr.server.IndexRConfig;
import io.indexr.server.SegmentHelper;
import io.indexr.server.TableSchema;
import io.indexr.server.rt.RealtimeConfig;
import io.indexr.util.IOUtil;
import io.indexr.util.JsonUtil;
import io.indexr.util.RuntimeUtil;
import io.indexr.util.Strings;
import io.indexr.util.Trick;

/**
 * TODO handle the case when row number is larger then {@link StorageSegment#MAX_ROW_COUNT}.
 */
public class CSVSegmentLoader {
    private final TableSchema schema;
    private final ColumnSchema[] columnSchemas;
    private final int[] valIndexes;
    private final List<String> csvPaths;
    private final String spliter;

    private boolean interrupted;
    private long rowCount;

    private SegmentGen segmentGen;

    private static interface SegmentGen {
        void add(Row row) throws IOException;

        DPSegment gen() throws IOException;
    }

    public CSVSegmentLoader(String name,
                            TableSchema schema,
                            List<String> csvColNames,
                            List<String> csvPaths,
                            Path outPath,
                            String spliter,
                            SegmentMode mode) throws IOException {
        this.schema = schema;
        this.spliter = spliter;
        this.csvPaths = csvPaths;
        this.columnSchemas = new ColumnSchema[schema.schema.columns.size()];
        this.valIndexes = new int[schema.schema.columns.size()];
        for (int colId = 0; colId < schema.schema.columns.size(); colId++) {
            ColumnSchema cs = schema.schema.columns.get(colId);
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

        if (Trick.isEmpty(schema.aggSchema.dims)) {
            DPSegment segment = DPSegment.open(
                    Version.LATEST_ID,
                    schema.mode,
                    outPath,
                    name,
                    schema.schema,
                    OpenOption.Overwrite).update();
            this.segmentGen = new SegmentGen() {
                @Override
                public void add(Row row) throws IOException {
                    segment.add(row);
                }

                @Override
                public DPSegment gen() throws IOException {
                    segment.seal();
                    return segment;
                }
            };
        } else {
            SortedSegmentGenerator generator = new SortedSegmentGenerator(
                    Version.LATEST_ID,
                    schema.mode,
                    outPath,
                    name,
                    schema.schema,
                    schema.aggSchema.grouping,
                    schema.aggSchema.dims,
                    schema.aggSchema.metrics,
                    DataPack.MAX_COUNT * 10
            );
            this.segmentGen = new SegmentGen() {
                @Override
                public void add(Row row) throws IOException {
                    generator.add(row);
                }

                @Override
                public DPSegment gen() throws IOException {
                    return generator.seal();
                }
            };
        }
    }

    public CSVSegmentLoader(String name, TableSchema schema, List<String> csvColNames, List<String> csvPaths, Path outPath) throws IOException {
        this(name, schema, csvColNames, csvPaths, outPath, ",", SegmentMode.DEFAULT);
    }

    public DPSegment done() throws IOException {
        return segmentGen.gen();
    }

    public void load() throws IOException {
        if (!csvPaths.isEmpty()) {
            for (String file : csvPaths) {
                loadFile(file);
            }
        }
    }

    private void loadFile(String path) throws IOException {
        SimpleRow.Builder rowBuilder = SimpleRow.Builder.createByColumnSchemas(Arrays.asList(columnSchemas));
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"))) {
            String line;
            while (StringUtils.isNotEmpty(line = reader.readLine()) && !interrupted) {
                String[] vals = line.split(spliter);
                try {
                    for (int colId = 0; colId < schema.schema.columns.size(); colId++) {
                        int valIndex = valIndexes[colId];
                        rowBuilder.appendStringFormVal(getValue(vals, valIndex));
                    }
                } catch (NumberFormatException e) {
                    System.out.printf("error csv line: [%s]", line);
                    e.printStackTrace();
                }
                segmentGen.add(rowBuilder.buildAndReset());
                rowCount++;
            }
        }
    }

    private String getValue(String[] vals, int index) {
        return (index == -1 || Strings.isEmpty(vals[index])) ? "" : vals[index];
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
        @Option(name = "-mode", usage = "segment mode: storage, balance or performance")
        String mode = SegmentMode.DEFAULT.toString();
        @Option(name = "-ncp", usage = "[deprecated, use -mode instead] if set, will not compress segment. Default is compressed.")
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

    public static void main(String[] args) throws Exception {
        RealtimeConfig.loadSubtypes();
        MyOptions options = new MyOptions();
        CmdLineParser parser = RuntimeUtil.parseArgs(args, options);
        if (options.help) {
            parser.printUsage(System.out);
            return;
        }

        if (Strings.isEmpty(options.ouputPath)) {
            System.out.println("Please specify output path by -s");
        }

        Plugins.loadPlugins();
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

    private static void doLoad(IndexRConfig config, MyOptions options, String outputPath, Path tmpPath) throws Exception {
        TableSchema schema = options.schemaPath == null ? null : JsonUtil.load(Paths.get(options.schemaPath), TableSchema.class);
        Preconditions.checkState(options.schemaPath != null, "Please specify schema path by -C");
        Preconditions.checkState(schema.schema != null, "Illegal schame");

        List<String> csvColNames = null;
        if (options.headerPath != null) {
            csvColNames = Arrays.asList(IOUtils.toString(new FileInputStream(options.headerPath), "UTF-8").split(options.spliter));
        }
        List<String> csvPaths = options.csvPath == null ? Collections.emptyList() : IOUtil.wildcardFiles(options.csvPath);

        CSVSegmentLoader loader = new CSVSegmentLoader(
                "tmp_gen_name",
                schema,
                csvColNames,
                csvPaths,
                tmpPath,
                options.spliter,
                SegmentMode.fromNameWithCompress(options.mode, !options.notcompress));

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

        DPSegment segment = loader.done();

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

    static {
        // TODO Remove this
        RealtimeConfig.loadSubtypes();
    }
}
