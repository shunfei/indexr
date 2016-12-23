package io.indexr.util;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

public class RuntimeUtil {

    public static CmdLineParser parseArgs(String[] args, Object t) {
        CmdLineParser parser = new CmdLineParser(t);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            e.printStackTrace();
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            System.err.println();
            System.exit(1);
        }
        return parser;
    }

}
