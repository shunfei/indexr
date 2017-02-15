package io.indexr.tool;

import org.apache.directory.api.util.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import io.indexr.server.rt2his.Rt2HisOnHive;
import io.indexr.util.RuntimeUtil;

public class Rt2His {
    private static final Logger logger = LoggerFactory.getLogger(Rt2His.class);

    private static class MyOptions {
        @Option(name = "-h", usage = "print this help. ")
        boolean help;

        @Option(name = "-table", metaVar = "<tableName>", usage = "hive table name")
        String table;
        @Option(name = "-hivecnn", metaVar = "<connect>", usage = "hive server2 connection string. e.g. 'jdbc:hive2://localhost:10000/default;auth=noSasl'")
        String hiveConnection;
        @Option(name = "-hiveuser", metaVar = "<user>", usage = "hive connection user")
        String hiveUser;
        @Option(name = "-hivepwd", metaVar = "<pwd>", usage = "hive connection password")
        String hivePwd;
        @Option(name = "-segpc", metaVar = "<sql>", usage = "the partition column or sql in segment. " +
                "Node that it is NOT the hive table's partition column. e.g `date` or substr(`date`, 0, 8)")
        String partitionColumn;
        @Option(name = "-select", metaVar = "<sql>", usage = "optional. The select sql used to fetch realtime data in the way you like. " +
                "If not specify, only move raw rows into corresponding partitions without any modification. " +
                "It must contains {TABLE} and {WHERE} place holder. " +
                "e.g. SELECT c0, c1, SUM(c2) from {TABLE} where {WHERE} GROUP BY c0, c1 ORDER BY c0 ASC, c1 DESC")
        String selectSql;

        @Override
        public String toString() {
            return "Options{" +
                    "help=" + help +
                    ", table='" + table + '\'' +
                    ", hivecnn='" + hiveConnection + '\'' +
                    ", hiveuser='" + hiveUser + '\'' +
                    ", hivepwd='" + hivePwd + '\'' +
                    ", segpc='" + partitionColumn + '\'' +
                    ", select='" + selectSql + '\'' +
                    '}';
        }
    }

    public static FileSystem getFileSystem() {
        Configuration config = new Configuration();
        config.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        try {
            return FileSystem.get(config);
        } catch (IOException e) {
            throw new RuntimeException("Initialize file system failed!", e);
        }
    }

    private static boolean hiveMode(MyOptions options) {
        FileSystem fileSystem = getFileSystem();
        if (!(fileSystem instanceof DistributedFileSystem)) {
            String msg = "Defaul file system is not DistributedFileSystem. Forget include hadoop config files (core-site.xml, hdfs-site.xml) into classpath?";
            System.out.println(msg);
            return false;
        }
        if (Strings.isEmpty(options.hiveConnection)) {
            System.out.println("please specify hive connection by -hive");
            return false;
        }
        if (Strings.isEmpty(options.table)) {
            System.out.println("please specify hive table name by -table");
            return false;
        }
        if (Strings.isEmpty(options.partitionColumn)) {
            System.out.println("please specify segment partition column by -segpc");
            return false;
        }
        Rt2HisOnHive rt2HisOnHive = new Rt2HisOnHive(
                fileSystem,
                options.hiveConnection,
                options.hiveUser,
                options.hivePwd,
                options.table,
                options.partitionColumn,
                options.selectSql
        );
        return rt2HisOnHive.handle();
    }

    public static void main(String[] args) {
        MyOptions options = new MyOptions();
        CmdLineParser parser = RuntimeUtil.parseArgs(args, options);
        if (options.help) {
            parser.printUsage(System.out);
            return;
        }

        if (!hiveMode(options)) {
            logger.info("Failed. options: {}", options.toString());
            System.exit(1);
        } else {
            logger.info("Succeed. options: {}", options.toString());
        }
    }

}
