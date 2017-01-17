package io.indexr.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class should only be used in config loading or debug.
 * Don't use in high performance required scenario.
 */
public class JsonUtil {
    private static final Pattern ExtendFlag = Pattern.compile("\"@extend:.*?\"");
    public static final ObjectMapper jsonMapper = new ObjectMapper();
    public static final ObjectMapper jsonMapperPretty = new ObjectMapper();

    static {
        jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        jsonMapper.configure(SerializationFeature.INDENT_OUTPUT, false);

        jsonMapperPretty.configure(SerializationFeature.INDENT_OUTPUT, true);
    }

    public static String toJson(Object v) {
        try {
            return jsonMapper.writeValueAsString(v);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static String toJsonPretty(Object v) {
        try {
            return jsonMapperPretty.writeValueAsString(v);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }


    public static byte[] toJsonBytes(Object v) {
        try {
            return jsonMapper.writeValueAsBytes(v);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T fromJson(String json, Class<T> clazz) {
        try {
            return jsonMapper.readValue(json, clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T fromJson(byte[] bytes, Class<T> clazz) {
        try {
            return jsonMapper.readValue(bytes, clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T loadWithRE(Path path, Class<T> clazz) {
        try {
            return load(path, clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T load(Path path, Class<T> clazz) throws IOException {
        try (InputStream in = Channels.newInputStream(FileChannel.open(path, StandardOpenOption.READ))) {
            return jsonMapper.readValue(in, clazz);
        }
    }

    public static <T> T loadResource(String filePath, Class<T> clazz) throws IOException {
        try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(filePath)) {
            return jsonMapper.readValue(in, clazz);
        }
    }

    public static <T> T load(Path path, TypeReference ref) throws IOException {
        try (InputStream in = Channels.newInputStream(FileChannel.open(path, StandardOpenOption.READ))) {
            return jsonMapper.readValue(in, ref);
        }
    }

    public static void saveWithRE(Path path, Object val) {
        try {
            save(path, val);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void save(Path path, Object val) throws IOException {
        try (FileChannel file = FileChannel.open(path,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            jsonMapper.writeValue(Channels.newOutputStream(file), val);
        }
    }

    public static <T> T loadConfig(Path path, Class<T> clazz) throws IOException {
        return jsonMapper.readValue(readJsonFile(path), clazz);
    }

    public static <T> T loadConfig(Path path, TypeReference ref) throws IOException {
        return jsonMapper.readValue(readJsonFile(path), ref);
    }

    private static String readJsonFile(Path path) throws IOException {
        File configFile = path.toFile();
        if (!configFile.isFile() && !configFile.canRead()) {
            throw new IOException(String.format("json file[%s] invalid!", path));
        }
        String dir = configFile.getParent();
        String content;
        try (FileInputStream is = new FileInputStream(configFile)) {
            content = IOUtils.toString(is);
        }

        Matcher matcher = ExtendFlag.matcher(content);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String reg = matcher.group();
            String replaceFile = StringUtils.removeStart(StringUtils.strip(reg, "\"").trim(), "@extend:").trim();
            if (!replaceFile.startsWith("/") && dir != null) {
                replaceFile = dir + "/" + replaceFile;
            }
            String replaceJson = readJsonFile(Paths.get(replaceFile));
            matcher.appendReplacement(sb, replaceJson);
        }
        matcher.appendTail(sb);
        return sb.toString();
    }
}
