package io.indexr.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class IndexRInputFormat extends FileInputFormat<Void, ArrayWritable> implements CombineHiveInputFormat.AvoidSplitCombination {
    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {
        //  Never splite.
        return false;
    }

    @Override
    public boolean shouldSkipCombine(Path path, Configuration conf) throws IOException {
        // Never combine.
        return true;
    }

    @Override
    public RecordReader getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
        return new IndexRRecordReader(inputSplit, jobConf);
    }
}
