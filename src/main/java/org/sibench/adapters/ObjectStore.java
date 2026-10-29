package org.sibench.adapters;

import static org.sibench.config.ConfigValues.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

/**
 * Where an object-storage index keeps its objects: one profile of config/storage.yaml and the
 * object path of this index below the profile's prefix. Credentials are read from the environment
 * variables the profile names and are never printed.
 */
record ObjectStore(
        String profile,
        String endpoint,
        String region,
        String bucket,
        String objectPath,
        boolean virtualHosted,
        String accessKey,
        String secretKey,
        String sessionToken) {
    static ObjectStore of(Path storageConfig, String profile, String objectPath) throws IOException {
        Map<String, Object> profiles = object(parse(Files.readString(storageConfig)), "profiles");
        require(profiles.containsKey(profile), "Unknown storage profile: " + profile);
        var store = object(profiles, profile);
        for (String key : store.keySet())
            require(
                    Set.of(
                                    "kind", "endpoint", "region", "bucket", "prefix", "addressing",
                                    "versioning", "access_key_env", "secret_key_env",
                                    "session_token_env")
                            .contains(key),
                    "Unknown storage key: " + key);
        require(string(store, "kind").equals("s3"), "Only S3-compatible storage is supported");
        String prefix = string(store, "prefix");
        require(
                objectPath.startsWith(prefix) && objectPath.length() > prefix.length(),
                "object_path must lie below the profile prefix " + prefix);
        String addressing = string(store, "addressing");
        require(
                addressing.equals("virtual_hosted") || addressing.equals("path"),
                "addressing must be virtual_hosted or path");
        return new ObjectStore(
                profile,
                string(store, "endpoint"),
                string(store, "region"),
                string(store, "bucket"),
                objectPath,
                addressing.equals("virtual_hosted"),
                secret(store, "access_key_env", true),
                secret(store, "secret_key_env", true),
                store.containsKey("session_token_env")
                        ? secret(store, "session_token_env", false)
                        : "");
    }

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
