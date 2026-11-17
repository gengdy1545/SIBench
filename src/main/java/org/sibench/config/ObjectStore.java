package org.sibench.config;

import static org.sibench.config.ConfigValues.*;

import java.io.IOException;
import java.nio.file.Path;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Shared object-storage connections. URI schemes select AWS or COS; credentials are
 * resolved only when opening a connection and never included in recorded settings.
 */
public record ObjectStore(
        String profile,
        String endpoint,
        String region,
        String bucket,
        String objectPath,
        boolean virtualHosted,
        String accessKey,
        String secretKey,
        String sessionToken) {
    public static ObjectStore of(Path runtime, String location) throws IOException {
        return of(SibenchConfig.read(runtime), location);
    }

    public static ObjectStore of(Map<String, Object> runtime, String location) throws IOException {
        return connect(configuration(runtime, location));
    }

    public static URI uri(String location) {
        URI uri = URI.create(location);
        require(uri.getScheme() != null && Set.of("s3", "cos").contains(uri.getScheme()), "Object path must use s3:// or cos://");
        require(uri.getHost() != null && uri.getHost().matches("[a-z0-9][a-z0-9.-]*[a-z0-9]")
                && uri.getRawUserInfo() == null && uri.getPort() == -1 && uri.getRawQuery() == null
                && uri.getRawFragment() == null, "Invalid object-storage bucket URI");
        String path = uri.getRawPath();
        require(path != null && path.matches("/[A-Za-z0-9_.-]+(/[A-Za-z0-9_.-]+)*/?"),
                "Object URI needs a nonempty canonical path");
        for (String part : path.substring(1).split("/"))
            require(!part.equals(".") && !part.equals(".."), "Object path traversal");
        return uri;
    }

    public static void validate(Map<String, Object> stores) {
        for (String name : stores.keySet()) {
            require(Set.of("aws", "cos").contains(name), "Unknown object-storage connection: " + name);
            var store = object(stores, name);
            for (String key : store.keySet())
                require(Set.of("endpoint", "region", "addressing", "access_key_env",
                        "secret_key_env", "session_token_env").contains(key), "Unknown storage key: " + key);
            URI endpoint = URI.create(string(store, "endpoint"));
            require(endpoint.getScheme() != null && Set.of("https", "http").contains(endpoint.getScheme()) && endpoint.getHost() != null
                    && endpoint.getRawUserInfo() == null && endpoint.getRawQuery() == null
                    && endpoint.getRawFragment() == null && (endpoint.getPath().isEmpty() || endpoint.getPath().equals("/")),
                    "Invalid object-storage endpoint");
            require(!string(store, "region").isBlank(), "Storage region must not be blank");
            require(Set.of("virtual_hosted", "path").contains(string(store, "addressing")), "Invalid storage addressing");
            for (String key : Set.of("access_key_env", "secret_key_env", "session_token_env"))
                if (!key.equals("session_token_env") || store.containsKey(key))
                    require(string(store, key).matches("[A-Za-z_][A-Za-z0-9_]*"), "Invalid credential variable: " + key);
        }
    }

    /** A transportable configuration containing variable names, never credential values. */
    public static Map<String, Object> configuration(Map<String, Object> runtime, String location) {
        URI uri = uri(location);
        String name = uri.getScheme().equals("s3") ? "aws" : "cos";
        var store = new LinkedHashMap<>(object(object(runtime, "object_storage"), name));
        validate(Map.of(name, store));
        store.put("uri", location);
        store.put("bucket", uri.getHost());
        String path = uri.getPath().substring(1);
        store.put("prefix", path.endsWith("/") ? path.substring(0, path.length() - 1) : path);
        return java.util.Collections.unmodifiableMap(store);
    }

    public static ObjectStore connect(Map<String, Object> store) throws IOException {
        URI uri = uri(string(store, "uri"));
        var connection = new LinkedHashMap<>(store);
        connection.keySet().removeAll(Set.of("uri", "bucket", "prefix"));
        var checked = configuration(Map.of("object_storage", Map.of(uri.getScheme().equals("s3") ? "aws" : "cos", connection)), uri.toString());
        require(checked.equals(store), "Object-storage URI and resolved settings differ");
        String addressing = string(store, "addressing");
        return new ObjectStore(
                uri.getScheme().equals("s3") ? "aws" : "cos",
                string(store, "endpoint"),
                string(store, "region"),
                string(store, "bucket"),
                string(store, "prefix"),
                addressing.equals("virtual_hosted"),
                secret(store, "access_key_env", true),
                secret(store, "secret_key_env", true),
                store.containsKey("session_token_env")
                        ? secret(store, "session_token_env", false)
                        : "");
    }

    public String location() { return (profile.equals("aws") ? "s3" : "cos") + "://" + bucket + "/" + objectPath; }

    private static String secret(Map<String, Object> store, String key, boolean required)
            throws IOException {
        String variable = string(store, key);
        String value = System.getenv(variable);
        if (value == null || value.isEmpty()) {
            if (required) throw new IOException("Environment variable " + variable + " is not set");
            return "";
        }
        return value;
    }

    /** Everything but the credentials. */
    @Override
    public String toString() {
        return "ObjectStore[profile="
                + profile
                + ", endpoint="
                + endpoint
                + ", region="
                + region
                + ", bucket="
                + bucket
                + ", objectPath="
                + objectPath
                + ", virtualHosted="
                + virtualHosted
                + "]";
    }
}
