package io.indexr.util;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Properties;

/**
 * @author: sundy
 * @since 2016-03-14.
 */
public class PropertiesUtils {


    public static long getLong(Properties properties, String key) {
        return Long.parseLong(properties.getProperty(key));
    }

    public static Properties subSet(Properties properties, String prefix) {
        if (!prefix.endsWith(".")) {
            prefix = prefix + ".";
        }
        Properties p = new Properties();
        final String finalPrefix = prefix;

        properties.forEach((k, v) -> {
            String key = k.toString();
            if (key.startsWith(finalPrefix)) {
                p.put(key.substring(finalPrefix.length()), v);
            }
        });
        return p;
    }

    public static Properties loadRs(String s) throws IOException {
        Properties p = new Properties();
        p.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(s));
        return p;
    }

    public static Properties load(Path path) throws IOException {
        Properties p = new Properties();
        p.load(Channels.newInputStream(FileChannel.open(path, StandardOpenOption.READ)));
        return p;
    }
}
