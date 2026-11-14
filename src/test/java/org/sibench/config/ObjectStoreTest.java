package org.sibench.config;

import static org.junit.Assert.*;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Test;

public class ObjectStoreTest {
    private static Map<String, Object> connection(String endpoint) {
        return Map.of("endpoint", endpoint, "region", "test", "addressing", "path",
                "access_key_env", "SIBENCH_TEST_MISSING_KEY", "secret_key_env", "SIBENCH_TEST_MISSING_SECRET");
    }

    @Test public void uriRoutesBothProvidersWithoutReadingUnusedCredentials() throws Exception {
        var runtime = Map.<String, Object>of("object_storage", Map.of(
                "aws", connection("http://127.0.0.1:1"), "cos", connection("http://127.0.0.1:2")));
        var aws = ObjectStore.configuration(runtime, "s3://aws-bucket/traces/ycsb-a/");
        var cos = ObjectStore.configuration(runtime, "cos://cos-bucket/work/index/run");
        assertEquals("http://127.0.0.1:1", aws.get("endpoint"));
        assertEquals("aws-bucket", aws.get("bucket"));
        assertEquals("traces/ycsb-a", aws.get("prefix"));
        assertEquals("http://127.0.0.1:2", cos.get("endpoint"));
        assertEquals("cos-bucket", cos.get("bucket"));
        assertFalse(aws.containsKey("access_key"));
        assertThrows(java.io.IOException.class, () -> ObjectStore.of(runtime, "s3://aws-bucket/traces/a"));
    }

    @Test public void rejectsAmbiguousPathsAndDoesNotFallBackToAnotherProvider() {
        for (String path : new String[] {"relative/path", "/absolute/path", "https://bucket/a", "s3://bucket/", "s3://bucket", "s3://bucket/a/../b",
                "cos://bucket/a/./b", "s3://bucket/a//b", "s3://bucket/a%2fb", "cos://bucket/a?token=x",
                "s3://user@bucket/a", "s3://bucket:80/a", "cos://bucket/a#x", "s3://bucket/a\\b"})
            assertThrows(path, IllegalArgumentException.class, () -> ObjectStore.uri(path));
        assertThrows(IllegalArgumentException.class, () -> ObjectStore.configuration(
                Map.of("object_storage", Map.of("cos", connection("https://cos.example"))), "s3://bucket/a"));
        var connection = new LinkedHashMap<>(connection("https://example.com"));
        connection.put("bucket", "duplicate-owner");
        assertThrows(IllegalArgumentException.class, () -> ObjectStore.validate(Map.of("aws", connection)));
    }

    @Test public void nativeConnectionsResolveOnlySelectedVariablesAndRedactValues() throws Exception {
        String variable = "PATH";
        var connection = new LinkedHashMap<>(connection("https://example.com"));
        connection.put("access_key_env", variable); connection.put("secret_key_env", variable);
        var runtime = Map.<String, Object>of("object_storage", Map.of("aws", connection, "cos", connection("https://cos.example")));
        var store = ObjectStore.of(runtime, "s3://bucket/index/run/");
        assertEquals("aws", store.profile());assertEquals("s3://bucket/index/run", store.location());
        assertEquals(System.getenv(variable), store.accessKey());
        assertFalse(store.toString().contains(System.getenv(variable)));
        var tampered = new LinkedHashMap<>(ObjectStore.configuration(runtime, "s3://bucket/index/run"));
        tampered.put("bucket", "elsewhere");
        assertThrows(IllegalArgumentException.class, () -> ObjectStore.connect(tampered));
    }
}
