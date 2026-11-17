package org.sibench.adapters;

import static org.sibench.config.ConfigValues.*;

import java.util.Map;
import java.util.Set;

/**
 * Where a replay sends its operations ({@code target.service} of a replay config): any index
 * service speaking the gRPC protocol at {@code endpoint: host:port}, holding {@code value}
 * ({@code locator} or {@code row_id}) entries.
 */
public record ServiceTarget(String endpoint, ValueKind value) {
    public ServiceTarget {
        require(endpoint != null && !endpoint.isBlank(), "A replay target needs an endpoint");
        require(value != null, "A replay target needs a value kind");
    }

    public static ServiceTarget of(Map<String, Object> document) {
        var service = object(object(document, "target"), "service");
        for (String key : service.keySet())
            require(Set.of("endpoint", "value").contains(key), "Unknown key: " + key);
        return new ServiceTarget(string(service, "endpoint"), ServiceConfig.value(service));
    }
}
