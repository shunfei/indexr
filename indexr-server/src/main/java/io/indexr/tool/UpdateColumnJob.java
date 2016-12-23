package io.indexr.tool;

import com.google.common.collect.Lists;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.directory.api.util.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.indexr.io.ByteBufferReader;
import io.indexr.segment.ColumnType;
import io.indexr.segment.pack.IntegratedSegment;
import io.indexr.segment.pack.StorageSegment;
import io.indexr.segment.pack.UpdateColSchema;
import io.indexr.segment.pack.UpdateColSegment;
import io.indexr.server.IndexRConfig;
import io.indexr.server.SegmentHelper;
import io.indexr.util.JsonUtil;
import io.indexr.util.RuntimeUtil;
import io.indexr.util.Try;

/**
 * 1. Map: Create new segments, and upload them to a temporary dir.
 * 2. Job commit: Remove the old segments and move in new segments from temp dir.
 * 3. Job commit: Notify segment updates to indexr.
 */
public class UpdateColumnJob extends Configured implements Tool {
    private static final Logger log = LoggerFactory.getLogger(UpdateColumnJob.class);

    private static final String MODE_ADD = "ADDCOL";
    private static final String MODE_DELETE = "DELCOL";
    private static final String MODE_ALTER = "ALTCOL";

    private static final String TMP_SEG_DIR = "segment";
    private static final String TMP_SEG_POSFIX = ".UPCOL";
    private static final String CONFKEY = "io.indexr.tool.updateColumn";

    private static final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm");

    public static class Config {
        @JsonProperty("table")
        public final String table;
        @JsonProperty("tableRoot")
        public final String tableRoot;
        @JsonProperty("mode")
        public final String mode;
        @JsonProperty("updateColumns")
        public final List<UpdateColSchema> upadteColumns;

        public Config(@JsonProperty("table") String table,
                      @JsonProperty("tableRoot") String tableRoot,
                      @JsonProperty("mode") String mode,
                      @JsonProperty("updateColumns") List<UpdateColSchema> upadteColumns) {
            this.table = table;
            this.tableRoot = tableRoot;
            this.mode = mode;
            this.upadteColumns = upadteColumns;
        }
    }

    public static class SegmentInputFormat extends InputFormat<String, String> {
        @Override
        public List<InputSplit> getSplits(final JobContext jobContext) throws IOException, InterruptedException {
            FileSystem fileSystem = FileSystem.get(jobContext.getConfiguration());
            Config config = JsonUtil.fromJson(jobContext.getConfiguration().get(CONFKEY), Config.class);

            List<InputSplit> segmentSplits = new ArrayList<>();
            Path rootPath = new Path(config.tableRoot);
            rootPath = fileSystem.resolvePath(rootPath);
            String dirStr = rootPath.toString() + "/";
            SegmentHelper.literalAllSegments(fileSystem, rootPath, f -> {
                String segmentPath = f.getPath().toString();
                long segmentSize = f.getLen();
                segmentSplits.add(new SegmentSplit(new SegmentFile(
                        segmentPath,
                        StringUtils.removeStart(segmentPath, dirStr),
                        segmentSize)));
            });
            return segmentSplits;
        }

        @Override
        public RecordReader<String, String> createRecordReader(
                final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext
        ) throws IOException, InterruptedException {
            return new RecordReader<String, String>() {
                // @formatter:off
                boolean readAnything = false;
                @Override public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {}
                @Override public boolean nextKeyValue() throws IOException, InterruptedException {return !readAnything;}
                @Override public String getCurrentKey() throws IOException, InterruptedException {return "key";}
                @Override public String getCurrentValue() throws IOException, InterruptedException {readAnything = true;return "fakeValue";}
                @Override public float getProgress() throws IOException, InterruptedException {return readAnything ? 0.0F : 1.0F;}
                @Override public void close() throws IOException {}
                // @formatter:on
            };
        }
    }

    public static class SegmentFile {
        @JsonProperty("path")
        public final String path;
        @JsonProperty("name")
        public final String name;
        @JsonProperty("size")
        public final long size;

        public SegmentFile(@JsonProperty("path") String path,
                           @JsonProperty("name") String name,
                           @JsonProperty("size") long size) {
            this.path = path;
            this.name = name;
            this.size = size;
        }
    }


    public static class SegmentSplit extends InputSplit implements Writable {
        private SegmentFile segmentFile;

        // @formatter:off
        public SegmentSplit() {}
        public SegmentSplit(SegmentFile segmentFile) {this.segmentFile = segmentFile;}
        @Override public long getLength() throws IOException, InterruptedException {return segmentFile.size;}
        @Override public String[] getLocations() throws IOException, InterruptedException {return new String[]{};}
        @Override public void write(DataOutput out) throws IOException {out.write(JsonUtil.toJsonBytes(segmentFile));}
        @Override public void readFields(DataInput in) throws IOException {segmentFile = JsonUtil.fromJson(in.readLine(), SegmentFile.class);}
        // @formatter:on
    }


    public static class UpColSegmentOutputFormat extends OutputFormat<Text, Text> {
        private static final Logger log = LoggerFactory.getLogger(UpColSegmentOutputFormat.class);

        @Override
        public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
            return new RecordWriter<Text, Text>() {
                @Override
                public void write(Text key, Text value) throws IOException, InterruptedException {}

                @Override
                public void close(TaskAttemptContext context) throws IOException, InterruptedException {}
            };
        }

        @Override
        public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {}

        @Override
        public OutputCommitter getOutputCommitter(final TaskAttemptContext context)
                throws IOException, InterruptedException {
            return new OutputCommitter() {
                // @formatter:off
                @Override public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {return false;}
                @Override public void setupJob(JobContext jobContext) throws IOException {}
                @Override public void setupTask(TaskAttemptContext taskContext) throws IOException {}
                @Override public void commitTask(TaskAttemptContext taskContext) throws IOException {}
                @Override public void abortTask(TaskAttemptContext taskContext) throws IOException {}
                // @formatter:on

                @Override
                public void commitJob(JobContext jobContext) throws IOException {
                    Config config = JsonUtil.fromJson(jobContext.getConfiguration().get(CONFKEY), Config.class);

                    DistributedFileSystem fileSystem = (DistributedFileSystem) FileSystem.get(jobContext.getConfiguration());
                    Path tmpSegDir = new Path(jobContext.getWorkingDirectory(), TMP_SEG_DIR);
                    try {
                        tmpSegDir = fileSystem.resolvePath(tmpSegDir);
                        String tmpSegDirStr = tmpSegDir.toString() + "/";
                        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(tmpSegDir, true);
                        while (files.hasNext()) {
                            LocatedFileStatus fileStatus = files.next();
                            if (!fileStatus.isFile()) {
                                continue;
                            }
                            Path tmpSegmentPath = fileStatus.getPath();
                            if (!tmpSegmentPath.getName().endsWith(TMP_SEG_POSFIX)) {
                                continue;
                            }

                            String tmpSegmentName = StringUtils.removeStart(tmpSegmentPath.toString(), tmpSegDirStr);
                            String oldSegmentName = StringUtils.removeEnd(tmpSegmentName, TMP_SEG_POSFIX);
                            Path oldSegmentPath = new Path(config.tableRoot, oldSegmentName);
                            if (!fileSystem.exists(oldSegmentPath)) {
                                throw new RuntimeException(String.format(
                                        "Old segment not exist! config.tableRoot:[%s], oldSegmentName:[%s], oldSegmentPath:[%s]",
                                        config.tableRoot, oldSegmentName, oldSegmentPath));
                            }

                            // Remove the old segments, then move in new ones with new names.

                            boolean ok = fileSystem.delete(oldSegmentPath, false);
                            if (!ok) {
                                throw new RuntimeException("Fail to delete old segment: " + oldSegmentPath.toString());
                            }

                            String newSegmentName = String.format(
                                    "%s_%s_%s",
                                    config.mode,
                                    LocalDateTime.now().format(timeFormatter),
                                    RandomStringUtils.randomAlphabetic(16));
                            Path newSegmentPath = new Path(oldSegmentPath.getParent(), newSegmentName);
                            ok = fileSystem.rename(tmpSegmentPath, newSegmentPath);
                            if (!ok) {
                                throw new RuntimeException(String.format("Fail to rename from [%s] to [%s]",
                                        tmpSegmentPath,
                                        newSegmentPath));
                            }
                        }
                    } finally {
                        fileSystem.delete(jobContext.getWorkingDirectory(), true);
                        SegmentHelper.notifyUpdate(fileSystem, config.tableRoot);
                    }
                }

                @Override
                public void abortJob(JobContext jobContext, JobStatus.State state) throws IOException {
                    DistributedFileSystem fileSystem = (DistributedFileSystem) FileSystem.get(jobContext.getConfiguration());
                    fileSystem.delete(jobContext.getWorkingDirectory(), true);

                    Config config = JsonUtil.fromJson(jobContext.getConfiguration().get(CONFKEY), Config.class);
                    SegmentHelper.notifyUpdate(fileSystem, config.tableRoot);
                }
            };
        }
    }


    public static class UpColSegmentMapper extends Mapper<String, String, Text, Text> {
        private static final Logger log = LoggerFactory.getLogger(UpColSegmentMapper.class);

        @Override
        protected void map(
                String key, String value,
                Context context
        ) throws IOException, InterruptedException {
            context.setStatus("ANALYZING");
            context.progress();

            FileSystem fileSystem = FileSystem.get(context.getConfiguration());

            SegmentSplit split = (SegmentSplit) context.getInputSplit();
            Config config = JsonUtil.fromJson(context.getConfiguration().get(CONFKEY), Config.class);

            Path oldSegmentPath = new Path(split.segmentFile.path);
            String segmentName = split.segmentFile.name;
            Path tmpSegDir = new Path(context.getWorkingDirectory(), TMP_SEG_DIR);
            if (!fileSystem.exists(tmpSegDir)) {
                Try.on(() -> fileSystem.mkdirs(tmpSegDir), log);
            }
            Path uploadPath = new Path(tmpSegDir, segmentName + TMP_SEG_POSFIX);

            context.setStatus("OPENING");
            context.progress();

            ByteBufferReader.Opener opener = ByteBufferReader.Opener.create(fileSystem, oldSegmentPath);
            IntegratedSegment.Fd fd = IntegratedSegment.Fd.create(segmentName, opener);
            if (fd == null) {
                // This is not a valid segment file.
                return;
            }
            java.nio.file.Path localSegmentPath = Files.createTempDirectory("_segment_gen_");

            try (IntegratedSegment oldSegment = fd.open()) {
                context.setStatus("CREATING");
                context.progress();

                StorageSegment newSegment;
                String newName = segmentName + TMP_SEG_POSFIX;
                switch (config.mode) {
                    case MODE_ADD:
                        newSegment = UpdateColSegment.addColumn(newName, oldSegment, config.upadteColumns, localSegmentPath);
                        break;
                    case MODE_DELETE:
                        newSegment = UpdateColSegment.deleteColumn(newName, oldSegment, Lists.transform(config.upadteColumns, c -> c.name));
                        break;
                    case MODE_ALTER:
                        newSegment = UpdateColSegment.alterColumn(newName, oldSegment, config.upadteColumns, localSegmentPath);
                        break;
                    default:
                        throw new RuntimeException("illegal mode: " + config.mode);
                }
                if (newSegment != oldSegment) {
                    context.setStatus("UPLOADING");
                    context.progress();

                    SegmentHelper.uploadSegment(newSegment, fileSystem, uploadPath, false);
                } else {
                    // This segment doesn't need to update.
                }

                context.setStatus("DONE");
            } finally {
                FileUtils.deleteDirectory(localSegmentPath.toFile());
            }
        }
    }


    public boolean doRun(Config upcolConfig) throws Exception {
        JobConf jobConf = new JobConf(getConf(), UpdateColumnJob.class);
        jobConf.setKeepFailedTaskFiles(false);
        jobConf.setNumReduceTasks(0);
        String jobName = String.format("indexr-upcol-%s-%s-%s",
                upcolConfig.table,
                LocalDateTime.now().format(timeFormatter),
                RandomStringUtils.randomAlphabetic(5));
        jobConf.setJobName(jobName);
        jobConf.set(CONFKEY, JsonUtil.toJson(upcolConfig));
        Path workDir = new Path(jobConf.getWorkingDirectory(), jobName);
        jobConf.setWorkingDirectory(workDir);

        Job job = Job.getInstance(jobConf);
        job.setInputFormatClass(SegmentInputFormat.class);
        job.setMapperClass(UpColSegmentMapper.class);
        job.setJarByClass(UpdateColumnJob.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapSpeculativeExecution(false);
        job.setOutputFormatClass(UpColSegmentOutputFormat.class);

        job.submit();
        boolean ok = job.waitForCompletion(true);
        if (!ok) {
            TaskReport[] reports = job.getTaskReports(TaskType.MAP);
            if (reports != null) {
                for (TaskReport report : reports) {
                    log.error("Error in task [%s] : %s", report.getTaskId(), Arrays.toString(report.getDiagnostics()));
                }
            }
        }
        return ok;
    }

    @Override
    public int run(String[] args) throws Exception {
        MyOptions options = new MyOptions();
        CmdLineParser parser = RuntimeUtil.parseArgs(args, options);
        if (options.help) {
            parser.printUsage(System.out);
            return 0;
        }
        if (Strings.isEmpty(options.table)) {
            System.out.println("please specify -t");
            return 1;
        }
        if (options.columns == null || options.columns.trim().isEmpty()) {
            System.out.println("please specify -col");
            return 1;
        }
        String columns = options.columns.trim();

        String mode;
        List<UpdateColSchema> updateColSchemas;
        if (options.add) {
            mode = MODE_ADD;
            updateColSchemas = JsonUtil.jsonMapper.readValue(columns, new TypeReference<List<UpdateColSchema>>() {});
        } else if (options.alter) {
            mode = MODE_ALTER;
            updateColSchemas = JsonUtil.jsonMapper.readValue(columns, new TypeReference<List<UpdateColSchema>>() {});
        } else if (options.delete) {
            mode = MODE_DELETE;
            try {
                updateColSchemas = JsonUtil.jsonMapper.readValue(columns, new TypeReference<List<UpdateColSchema>>() {});
            } catch (JsonProcessingException e) {
                // this schema is not specified by json.
                String[] strs = columns.split(",");
                updateColSchemas = Lists.transform(Arrays.asList(strs), s -> new UpdateColSchema(s, ColumnType.STRING)); // type is not used.
            }
        } else {
            System.out.println("please specify update mode -[add|del|alt]");
            return 1;
        }
        if (updateColSchemas.isEmpty()) {
            System.out.println("no update schemas");
            return 1;
        }

        System.out.printf("table: %s, mode: %s, columns: %s\n", options.table, mode, JsonUtil.toJson(updateColSchemas));

        IndexRConfig indexrConf = new IndexRConfig();
        String segmentRoot = IndexRConfig.segmentRootPath(indexrConf.getDataRoot(), options.table);
        Config config = new Config(
                options.table,
                segmentRoot,
                mode,
                updateColSchemas);
        return doRun(config) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new UpdateColumnJob(), args));
    }

    private static class MyOptions {
        @Option(name = "-h", usage = "print this help. Usage: <cmd> -t <table> -[add|del|alt] -col <cols>")
        boolean help;

        @Option(name = "-t", metaVar = "<table>", usage = "indexr table name")
        String table;
        @Option(name = "-add", usage = "add columns")
        boolean add;
        @Option(name = "-del", usage = "delete columns")
        boolean delete;
        @Option(name = "-alt", usage = "alter columns")
        boolean alter;
        @Option(name = "-col", metaVar = "<cols>", usage = "for del mode, list the column names, e.g. old_col,old_col2;" +
                "\nfor add or alt mode, specify column schemas, e.g." +
                "\n[{\"name\": \"new_col1\", \"dataType\": \"long\", \"value\": \"old_col\"}," +
                "\n{\"name\": \"new_col2\", \"string\": \"long\", \"value\": \"cast(old_col + old_col2, string)\"}]")
        String columns;
    }
}
