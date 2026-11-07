package org.sibench.workloads;

import static org.junit.Assert.*;
import static org.sibench.config.ConfigValues.*;

import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;

public class ScaleInspectorTest {
    private static final Path ROOT = Path.of("").toAbsolutePath();

    private String ycsb() throws Exception {
        return Files.readString(ROOT.resolve("src/test/resources/config/ycsb_1k.yaml"));
    }

    private java.util.Map<String, Object> inspect(String text) throws Exception {
        Path profile = Files.createTempFile(ROOT.resolve("build/sibench"), "native-profile-", ".yaml");
        try {
            Files.writeString(profile, text);
            return ScaleInspector.inspect(ROOT, profile);
        } finally {
            Files.delete(profile);
        }
    }

    @Test
    public void nativeDefaultsHaveTheSameHandCheckedRowCount() throws Exception {
        for (String profile : ScaleInspector.DEFAULT_PROFILES) {
            var report = ScaleInspector.inspect(ROOT, Path.of(profile));
            assertEquals(87_060_000L, report.get("baseline_rows"));
            assertEquals("native", report.get("generation_mode"));
            assertFalse((Boolean) report.get("generation_verified"));
        }
    }

    @Test public void hybenchOverridesDriveBothLoaderResourceAndExpectedCounts() throws Exception {
        var config=parse(Files.readString(ROOT.resolve("config/benchmark/hybench.yaml")));
        var parameters=object(config,"native");parameters.put("sf","1x");
        parameters.put("parameters",java.util.Map.of("customer_number",300,"company_number",2,"transfer_number",6000,
                "checking_number",600,"loanapps_number",600,"loantrans_number",600));
        config.put("native",parameters);
        var values=HybenchParameters.read(ROOT.resolve("benchmark/HyBench-2024"),parameters);
        var rows=ScaleInspector.nativeRows(ROOT,config);
        assertEquals(java.util.Map.of("customer",300L,"company",2L,"savingaccount",302L,"checkingaccount",302L,
                "transfer",6000L,"checking",600L,"loanapps",600L,"loantrans",600L),rows);
        assertEquals(8706L,rows.values().stream().mapToLong(Long::longValue).sum());
        String text=new com.moandjiezana.toml.TomlWriter().write(java.util.Map.of("1x",values));
        var loaded=new com.moandjiezana.toml.Toml().read(text).getTable("1x");
        assertEquals(Long.valueOf(300),loaded.getLong("customer_number"));
        assertEquals(Long.valueOf(6000),loaded.getLong("transfer_number"));
        assertEquals("customer.csv",loaded.getString("customer_datapath"));
        assertEquals(Double.valueOf(0.8),loaded.getDouble("cust_rate"));
        assertEquals(Long.valueOf(100),loaded.getLong("company_size_upper"));
        assertEquals(values,loaded.toMap());
        for(var invalid:java.util.List.of(java.util.Map.of("customer_number",0),java.util.Map.of("customer_number",1.5),
                java.util.Map.of("customer_datapath","elsewhere"),java.util.Map.of("unknown",1))) {
            parameters.put("parameters",invalid);
            assertThrows(IllegalArgumentException.class,()->HybenchParameters.read(ROOT.resolve("benchmark/HyBench-2024"),parameters));
        }
        parameters.remove("parameters");parameters.put("sf","999x");
        assertThrows(IllegalArgumentException.class,()->HybenchParameters.read(ROOT.resolve("benchmark/HyBench-2024"),parameters));
    }

    @Test
    public void rejectsUnsupportedPurposeAndVersion() throws Exception {
        String source = ycsb();
        for (String invalid : new String[] {
                source.replace("schema_version: 1", "schema_version: 2"),
                source.replace("purpose: native_capture", "purpose: baseline_generation")})
            assertThrows(IllegalArgumentException.class, () -> inspect(invalid));
    }

    @Test
    public void preservesNativeInt64CountsAndRejectsOverflow() throws Exception {
        assertEquals(3_000_000_000L, inspect(ycsb().replace("recordcount: 1000", "recordcount: 3000000000"))
                .get("baseline_rows"));
        assertThrows(ArithmeticException.class, () -> inspect(ycsb()
                .replace("recordcount: 1000", "recordcount: 9223372036854775808")));
        String sysbench = Files.readString(ROOT.resolve("src/test/resources/config/sysbench_2k.yaml"));
        assertThrows(ArithmeticException.class, () -> inspect(sysbench
                .replace("table-size: 1000", "table-size: 9223372036854775807")));
    }

    @Test
    public void rejectsInvalidCountsAndScalarTypes() throws Exception {
        String source = ycsb();
        for (String invalid : new String[] {"0", "true", "1.5"})
            assertThrows(IllegalArgumentException.class, () -> inspect(source
                    .replace("recordcount: 1000", "recordcount: " + invalid)));
    }

    @Test
    public void rejectsSourcePinChanges() throws Exception {
        String source = ycsb();
        String commit = string(object(parse(source), "source"), "commit");
        assertThrows(IllegalArgumentException.class, () -> inspect(source.replace(commit, "0".repeat(40))));
    }
}
