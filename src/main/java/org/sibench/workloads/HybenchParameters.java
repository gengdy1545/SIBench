package org.sibench.workloads;

import static org.sibench.config.ConfigValues.*;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Upstream parameters with explicit row-count overrides for bounded runs. */
public final class HybenchParameters {
    private static final Set<String> COUNTS = Set.of("customer_number", "company_number",
            "transfer_number", "checking_number", "loanapps_number", "loantrans_number");
    private HybenchParameters() {}

    public static void validate(Map<String, Object> nativeConfig) {
        if (!nativeConfig.containsKey("parameters")) return;
        var overrides = object(nativeConfig, "parameters");
        for (String key : overrides.keySet()) {
            require(COUNTS.contains(key), "Unsupported HyBench parameter override: " + key);
            positive(overrides, key);
        }
    }

    public static Map<String, Object> read(Path source, Map<String, Object> nativeConfig) {
        validate(nativeConfig);
        var defaults = new com.moandjiezana.toml.Toml()
                .read(source.resolve("src/main/resources/parameters.toml").toFile())
                .getTable(string(nativeConfig, "sf"));
        require(defaults != null, "Unknown HyBench scale");
        var parameters = new LinkedHashMap<String, Object>(defaults.toMap());
        if (nativeConfig.containsKey("parameters")) {
            var overrides = object(nativeConfig, "parameters");
            for (String key : overrides.keySet()) parameters.put(key, positive(overrides, key));
        }
        for (String key : COUNTS) positive(parameters, key);
        return java.util.Collections.unmodifiableMap(parameters);
    }

    public static Map<String, Long> rows(Map<String, Object> parameters) {
        var rows = new LinkedHashMap<String, Long>();
        rows.put("customer", positive(parameters, "customer_number"));
        rows.put("company", positive(parameters, "company_number"));
        long accounts = Math.addExact(rows.get("customer"), rows.get("company"));
        rows.put("savingaccount", accounts); rows.put("checkingaccount", accounts);
        for (String table : List.of("transfer", "checking", "loanapps", "loantrans"))
            rows.put(table, positive(parameters, table + "_number"));
        return java.util.Collections.unmodifiableMap(rows);
    }
}
