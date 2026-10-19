package org.sibench.config;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.Map;

public final class ConfigValues {
    private ConfigValues() {}

    public static Map<String, Object> parse(String text) {
        LoaderOptions options = new LoaderOptions();
        options.setAllowDuplicateKeys(false);
        return mapping(new Yaml(new SafeConstructor(options)).load(text), "configuration");
    }

    public static Map<String, Object> object(Map<String, Object> map, String key) {
        return mapping(map.get(key), key);
    }

    private static Map<String, Object> mapping(Object value, String name) {
        require(value instanceof Map<?, ?>, name + " must be a mapping");
        Map<String, Object> result = new LinkedHashMap<>();
        for (var entry : ((Map<?, ?>) value).entrySet()) {
            require(entry.getKey() instanceof String, name + " must have string keys");
            result.put((String) entry.getKey(), entry.getValue());
        }
        return result;
    }

    public static String string(Map<String, Object> map, String key) {
        require(map.get(key) instanceof String, key + " must be a string");
        return (String) map.get(key);
    }

    public static long integer(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value instanceof BigInteger big) return big.longValueExact();
        require(
                value instanceof Integer || value instanceof Long,
                key + " must be an int64 integer");
        return ((Number) value).longValue();
    }

    public static long positive(Map<String, Object> map, String key) {
        long value = integer(map, key);
        require(value > 0, key + " must be positive");
        return value;
    }

    public static boolean bool(Map<String, Object> map, String key) {
        require(map.get(key) instanceof Boolean, key + " must be boolean");
        return (Boolean) map.get(key);
    }

    public static void require(boolean condition, String message) {
        if (!condition) throw new IllegalArgumentException(message);
    }
}
