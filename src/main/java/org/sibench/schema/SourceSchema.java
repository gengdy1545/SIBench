package org.sibench.schema;

import static org.sibench.config.ConfigValues.*;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/** Reviewed layouts for the three pinned benchmark sources, not arbitrary SQL. */
public final class SourceSchema {
    public record DefaultValue(String kind, String value) {
        public static DefaultValue none() {
            return new DefaultValue("none", "");
        }

        public static DefaultValue sequence(String name) {
            return new DefaultValue("sequence", name);
        }
    }

    public record Column(String name, String type, boolean nullable, DefaultValue defaultValue) {}

    public record Index(String name, String column, boolean primary) {}

    public record Table(String name, List<Column> columns, List<Index> indexes) {
        public Table {
            columns = List.copyOf(columns);
            indexes = List.copyOf(indexes);
        }
    }

    private final String benchmark;
    private final String sourceCommit;
    private final Path source;
    private final List<Table> tables;
    private final Map<String, String> fingerprints;
    private final List<Index> secondaryOrder;
    private final Map<String, Object> nativeConfig;

    private SourceSchema(
            String benchmark,
            Path source,
            List<Table> tables,
            Map<String, String> fingerprints,
            Map<String, Object> nativeConfig) throws IOException {
        this(
                benchmark,
                source,
                tables,
                fingerprints,
                tables.stream()
                        .flatMap(t -> t.indexes().stream())
                        .filter(i -> !i.primary())
                        .toList(),
                nativeConfig);
    }

    private SourceSchema(
            String benchmark,
            Path source,
            List<Table> tables,
            Map<String, String> fingerprints,
            List<Index> secondaryOrder,
            Map<String, Object> nativeConfig) throws IOException {
        this(benchmark, source, tables, fingerprints, secondaryOrder, nativeConfig, null);
    }

    private SourceSchema(String benchmark, Path source, List<Table> tables, Map<String, String> fingerprints,
            List<Index> secondaryOrder, Map<String, Object> nativeConfig, String capturedCommit) throws IOException {
        this.benchmark = benchmark;
        try {
            this.sourceCommit = capturedCommit != null ? capturedCommit : Files.exists(source.resolve(".git"))
                    ? git(source, "rev-parse", "HEAD") : "unverified";
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted reading source identity", interrupted);
        }
        this.source = source;
        this.tables = List.copyOf(tables);
        // Insertion order is kept so that recorded provenance is byte-stable across runs.
        this.fingerprints = Collections.unmodifiableMap(new LinkedHashMap<>(fingerprints));
        this.secondaryOrder = List.copyOf(secondaryOrder);
        this.nativeConfig =
                nativeConfig == null
                        ? null
                        : Collections.unmodifiableMap(new LinkedHashMap<>(nativeConfig));
    }

    /** Layout and provenance bound by a published CDC artifact, independent of a source checkout. */
    public static SourceSchema captured(Path ddl, String sourceCommit, String expectedDigest) throws IOException {
        byte[] bytes;
        try (var input = Files.newInputStream(ddl)) { bytes = input.readNBytes((16 << 20) + 1); }
        String digest = HexFormat.of().formatHex(org.sibench.config.Digests.digest().digest(bytes));
        if (bytes.length > 16 << 20 || !digest.equals(expectedDigest)) throw new IOException("Captured source DDL changed");
        var layout = SimpleDdl.read(new String(bytes, StandardCharsets.UTF_8), "");
        return new SourceSchema("hybench", ddl.getParent(), layout.tables(),
                Map.of("schema.sql", digest), List.of(), null, sourceCommit);
    }

    public static SourceSchema read(Path root, Map<String, Object> config)
            throws IOException, InterruptedException {
        String benchmark = string(config, "benchmark");
        var declared = object(config, "source");
        require(Set.of("hybench", "ycsb", "sysbench").contains(benchmark), "Unsupported benchmark");
        root = root.toRealPath();
        String pin = git(root, "rev-parse", ":" + string(declared, "path"));
        require(pin.equals(string(declared, "commit")), "Source assertion differs from its submodule gitlink");
        Path source = root.resolve(string(declared, "path")).toRealPath();
        require(
                source.startsWith(root.resolve("benchmark").toRealPath()),
                "Schema source must be inside benchmark");
        require(git(source, "rev-parse", "HEAD").equals(pin), "Source commit mismatch");
        require(
                git(source, "status", "--porcelain=v1", "--untracked-files=all").isEmpty(),
                "Source worktree must be clean");
        return of(benchmark, source, benchmark.equals("hybench") ? null : object(config, "native"));
    }

    /**
     * The layout of an already verified source checkout; {@code nativeConfig} is the native client
     * configuration that shapes YCSB and sysbench tables and is null for HyBench.
     */
    public static SourceSchema of(String benchmark, Path source, Map<String, Object> nativeConfig)
            throws IOException {
        return switch (benchmark) {
            case "hybench" -> hybench(source);
            case "ycsb" -> ycsb(source, nativeConfig);
            case "sysbench" -> sysbench(source, nativeConfig);
            default -> throw new IllegalArgumentException("Unsupported benchmark");
        };
    }

    /** HyBench at a verified checkout: its layout comes from the DDL files alone. */
    public static SourceSchema hybench(Path source) throws IOException {
        var layout =
                SimpleDdl.read(
                        Files.readString(source.resolve("conf/ddl_pg.sql")),
                        Files.readString(source.resolve("conf/create_index_pg.sql")));
        return new SourceSchema(
                "hybench",
                source,
                layout.tables(),
                hashes(source, "conf/ddl_pg.sql", "conf/create_index_pg.sql"),
                layout.secondaryOrder(),
                null);
    }

    private static SourceSchema ycsb(Path source, Map<String, Object> nativeConfig)
            throws IOException {
        int fields = Math.toIntExact(positive(nativeConfig, "fieldcount"));
        require(fields <= 1024, "YCSB schema fieldcount exceeds 1024");
        String table =
                nativeConfig.containsKey("table") ? string(nativeConfig, "table") : "usertable";
        require(table.equals("usertable"), "Current YCSB layout requires usertable");
        var columns = new ArrayList<Column>();
        columns.add(new Column("ycsb_key", "character varying", false, DefaultValue.none()));
        for (int i = 0; i < fields; i++)
            columns.add(new Column("field" + i, "blob", true, DefaultValue.none()));
        return new SourceSchema(
                "ycsb",
                source,
                List.of(
                        new Table(
                                table,
                                columns,
                                List.of(new Index(table + "_pkey", "ycsb_key", true)))),
                hashes(
                        source,
                        "core/src/main/java/site/ycsb/DB.java",
                        "core/src/main/java/site/ycsb/workloads/CoreWorkload.java"),
                nativeConfig);
    }

    private static SourceSchema sysbench(Path source, Map<String, Object> nativeConfig)
            throws IOException {
        int count = Math.toIntExact(positive(nativeConfig, "tables"));
        require(count <= 10000, "sysbench schema table count exceeds 10000");
        require(
                !nativeConfig.containsKey("secondary") || !bool(nativeConfig, "secondary"),
                "sysbench secondary=true replaces the primary key and is unsupported");
        require(
                !nativeConfig.containsKey("create-secondary")
                        || bool(nativeConfig, "create-secondary"),
                "sysbench source must create all k secondary indexes");
        require(
                !nativeConfig.containsKey("pgsql-variant"),
                "sysbench pgsql-variant is unsupported by the DuckDB driver");
        require(
                !nativeConfig.containsKey("create-table-options")
                        || string(nativeConfig, "create-table-options").isEmpty(),
                "Custom sysbench table options are unsupported");
        boolean auto = !nativeConfig.containsKey("auto-inc") || bool(nativeConfig, "auto-inc");
        var tables = new ArrayList<Table>();
        for (int i = 1; i <= count; i++) {
            String table = "sbtest" + i;
            tables.add(
                    new Table(
                            table,
                            List.of(
                                    new Column(
                                            "id",
                                            "integer",
                                            false,
                                            auto
                                                    ? DefaultValue.sequence(table + "_id_seq")
                                                    : DefaultValue.none()),
                                    new Column(
                                            "k",
                                            "integer",
                                            false,
                                            new DefaultValue("integer", "0")),
                                    new Column(
                                            "c",
                                            "character(120)",
                                            false,
                                            new DefaultValue("string", "")),
                                    new Column(
                                            "pad",
                                            "character(60)",
                                            false,
                                            new DefaultValue("string", ""))),
                            List.of(
                                    new Index(table + "_pkey", "id", true),
                                    new Index("k_" + i, "k", false))));
        }
        return new SourceSchema(
                "sysbench", source, tables, hashes(source, "src/lua/oltp_common.lua"), nativeConfig);
    }

    private static String git(Path source, String... args)
            throws IOException, InterruptedException {
        var command = new ArrayList<>(List.of("git", "-C", source.toString()));
        command.addAll(List.of(args));
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        String value =
                new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        if (process.waitFor() != 0) throw new IOException("Cannot verify source: " + value);
        return value;
    }

    private static Map<String, String> hashes(Path source, String... paths) throws IOException {
        var result = new LinkedHashMap<String, String>();
        for (String path : paths) result.put(path, org.sibench.config.Digests.sha256(source.resolve(path)));
        return result;
    }

    public String benchmark() {
        return benchmark;
    }

    public String sourceCommit() {
        return sourceCommit;
    }

    public Path source() {
        return source;
    }

    public List<Table> tables() {
        return tables;
    }

    public Map<String, String> fingerprints() {
        return fingerprints;
    }

    public List<Index> secondaryOrder() {
        return secondaryOrder;
    }

    /** Null for HyBench, whose layout comes from its DDL files alone. */
    public Map<String, Object> nativeConfig() {
        return nativeConfig;
    }
}
