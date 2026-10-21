package org.sibench.schema;

import static org.junit.Assert.*;

import org.junit.Test;
import org.sibench.schema.PgSchemaCheck.*;
import org.sibench.schema.SourceSchema.*;

import java.util.*;

public class PgSchemaCheckTest {
    static List<ActualTable> matching(SourceSchema schema, String namespace) {
        return schema.tables().stream()
                .map(
                        table ->
                                new ActualTable(
                                        table.name(),
                                        "r",
                                        "p",
                                        false,
                                        false,
                                        table.columns().stream()
                                                .map(
                                                        column -> {
                                                            String expression =
                                                                    switch (column.defaultValue()
                                                                            .kind()) {
                                                                        case "none" -> null;
                                                                        case "sequence" ->
                                                                                "nextval('"
                                                                                        + column.defaultValue()
                                                                                                .value()
                                                                                        + "'::regclass)";
                                                                        case "current_timestamp" ->
                                                                                "CURRENT_TIMESTAMP";
                                                                        case "integer" ->
                                                                                column.defaultValue()
                                                                                        .value();
                                                                        case "string" ->
                                                                                "''::bpchar";
                                                                        default ->
                                                                                throw new AssertionError();
                                                                    };
                                                            String sequence =
                                                                    column.defaultValue()
                                                                                    .kind()
                                                                                    .equals(
                                                                                            "sequence")
                                                                            ? namespace
                                                                                    + "."
                                                                                    + column.defaultValue()
                                                                                            .value()
                                                                            : null;
                                                            String collation =
                                                                    column.type()
                                                                                            .startsWith(
                                                                                                    "character")
                                                                                    || column.type()
                                                                                            .equals(
                                                                                                    "text")
                                                                            ? "pg_catalog.default"
                                                                            : "";
                                                            return new ActualColumn(
                                                                    column.name(),
                                                                    column.type(),
                                                                    column.nullable(),
                                                                    expression,
                                                                    "",
                                                                    "",
                                                                    false,
                                                                    collation,
                                                                    sequence);
                                                        })
                                                .toList(),
                                        table.indexes().stream()
                                                .map(
                                                        index -> {
                                                            String type =
                                                                    table.columns().stream()
                                                                            .filter(
                                                                                    c ->
                                                                                            c.name()
                                                                                                    .equals(
                                                                                                            index
                                                                                                                    .column()))
                                                                            .findFirst()
                                                                            .orElseThrow()
                                                                            .type();
                                                            String op =
                                                                    switch (type) {
                                                                        case "integer" ->
                                                                                "pg_catalog.int4_ops";
                                                                        case "timestamp without"
                                                                                 + " time zone" ->
                                                                                "pg_catalog.timestamp_ops";
                                                                        case "character varying",
                                                                                        "text" ->
                                                                                "pg_catalog.text_ops";
                                                                        default ->
                                                                                "pg_catalog.bpchar_ops";
                                                                    };
                                                            return new ActualIndex(
                                                                    index.name(),
                                                                    index.column(),
                                                                    index.primary(),
                                                                    index.primary(),
                                                                    "btree",
                                                                    1,
                                                                    1,
                                                                    true,
                                                                    true,
                                                                    true,
                                                                    true,
                                                                    false,
                                                                    false,
                                                                    false,
                                                                    op,
                                                                    true,
                                                                    true,
                                                                    0,
                                                                    "CREATE INDEX "
                                                                            + index.name()
                                                                            + " ON "
                                                                            + table.name()
                                                                            + "("
                                                                            + index.column()
                                                                            + ")");
                                                        })
                                                .toList(),
                                        List.of()))
                .toList();
    }

    @Test
    public void allThreeReviewedLayoutsMatchIncludingNonPublicSequenceNamespace() throws Exception {
        for (String path :
                List.of(
                        "config/hybench_sf1.yaml",
                        "config/ycsb_large.yaml",
                        "config/sysbench_large.yaml")) {
            SourceSchema schema = SourceSchemaTest.read(path);
            assertTrue(PgSchemaCheck.compare(schema, matching(schema, "public")).isEmpty());
            assertTrue(PgSchemaCheck.compare(schema, matching(schema, "bench"), "bench").isEmpty());
        }
    }

    @Test
    public void missingExtraOrWrongTableShapeFails() throws Exception {
        SourceSchema schema = SourceSchemaTest.read("config/ycsb_large.yaml");
        List<ActualTable> valid = matching(schema, "public");
        assertDifference(schema, List.of(), "Missing table");
        assertDifference(
                schema,
                List.of(
                        valid.get(0),
                        new ActualTable(
                                "extra", "v", "p", false, false, List.of(), List.of(), List.of())),
                "Unexpected tables/views");
        var table = valid.get(0);
        for (ActualTable invalid :
                List.of(
                        new ActualTable(
                                table.name(),
                                "v",
                                "p",
                                false,
                                false,
                                table.columns(),
                                table.indexes(),
                                List.of()),
                        new ActualTable(
                                table.name(),
                                "r",
                                "u",
                                false,
                                false,
                                table.columns(),
                                table.indexes(),
                                List.of()),
                        new ActualTable(
                                table.name(),
                                "r",
                                "p",
                                true,
                                false,
                                table.columns(),
                                table.indexes(),
                                List.of()),
                        new ActualTable(
                                table.name(),
                                "r",
                                "p",
                                false,
                                true,
                                table.columns(),
                                table.indexes(),
                                List.of()),
                        new ActualTable(
                                table.name(),
                                "r",
                                "p",
                                false,
                                false,
                                table.columns(),
                                table.indexes(),
                                List.of("unexpected_check"))))
            assertFalse(PgSchemaCheck.compare(schema, List.of(invalid)).isEmpty());
    }

    @Test
    public void everyColumnPropertyAndFullColumnListAreChecked() throws Exception {
        SourceSchema schema = SourceSchemaTest.read("config/ycsb_large.yaml");
        ActualTable table = matching(schema, "public").get(0);
        var column = table.columns().get(0);
        List<ActualColumn> variants =
                List.of(
                        new ActualColumn(
                                "other",
                                column.type(),
                                false,
                                null,
                                "",
                                "",
                                false,
                                column.collation(),
                                null),
                        new ActualColumn(
                                column.name(),
                                "character varying(255)",
                                false,
                                null,
                                "",
                                "",
                                false,
                                column.collation(),
                                null),
                        new ActualColumn(
                                column.name(),
                                column.type(),
                                true,
                                null,
                                "",
                                "",
                                false,
                                column.collation(),
                                null),
                        new ActualColumn(
                                column.name(),
                                column.type(),
                                false,
                                "'fixed'",
                                "",
                                "",
                                false,
                                column.collation(),
                                null),
                        new ActualColumn(
                                column.name(),
                                column.type(),
                                false,
                                null,
                                "a",
                                "",
                                false,
                                column.collation(),
                                null),
                        new ActualColumn(
                                column.name(),
                                column.type(),
                                false,
                                null,
                                "",
                                "s",
                                false,
                                column.collation(),
                                null),
                        new ActualColumn(
                                column.name(),
                                column.type(),
                                false,
                                null,
                                "",
                                "",
                                true,
                                column.collation(),
                                null),
                        new ActualColumn(
                                column.name(),
                                column.type(),
                                false,
                                null,
                                "",
                                "",
                                false,
                                "pg_catalog.C",
                                null));
        for (ActualColumn changed : variants) {
            var columns = new ArrayList<>(table.columns());
            columns.set(0, changed);
            assertFalse(
                    PgSchemaCheck.compare(schema, List.of(withColumns(table, columns))).isEmpty());
        }
        assertDifference(
                schema,
                List.of(withColumns(table, table.columns().subList(0, 10))),
                "column count");
    }

    @Test
    public void sequenceDefaultsCannotBeReplacedOrBorrowedFromAnotherSchema() throws Exception {
        SourceSchema schema = SourceSchemaTest.read("config/sysbench_large.yaml");
        var tables = new ArrayList<>(matching(schema, "public"));
        ActualTable table = tables.get(0);
        var id = table.columns().get(0);
        for (ActualColumn changed :
                List.of(
                        new ActualColumn(
                                id.name(), id.type(), false, "42", "", "", false, "", null),
                        new ActualColumn(
                                id.name(),
                                id.type(),
                                false,
                                "nextval('sbtest1_id_seq'::regclass)",
                                "",
                                "",
                                false,
                                "",
                                "other.sbtest1_id_seq"),
                        new ActualColumn(
                                id.name(),
                                id.type(),
                                false,
                                "nextval('wrong'::regclass)",
                                "",
                                "",
                                false,
                                "",
                                "public.sbtest1_id_seq"))) {
            var columns = new ArrayList<>(table.columns());
            columns.set(0, changed);
            tables.set(0, withColumns(table, columns));
            assertFalse(PgSchemaCheck.compare(schema, tables).isEmpty());
        }
    }

    @Test
    public void allIndexSemanticsAndLifecycleFlagsAreCheckedNotJustNames() throws Exception {
        SourceSchema schema = SourceSchemaTest.read("config/sysbench_large.yaml");
        var tables = new ArrayList<>(matching(schema, "public"));
        ActualTable table = tables.get(0);
        ActualIndex index = table.indexes().get(1);
        for (int variant = 0; variant < 18; variant++) {
            ActualIndex changed =
                    new ActualIndex(
                            index.name(),
                            variant == 0 ? "id" : index.column(),
                            variant == 1,
                            variant == 2,
                            variant == 3 ? "hash" : "btree",
                            variant == 4 ? 2 : 1,
                            variant == 5 ? 2 : 1,
                            variant != 6,
                            variant != 7,
                            variant != 8,
                            variant != 9,
                            variant == 10,
                            variant == 11,
                            variant == 12,
                            variant == 13 ? "custom.int_ops" : index.operatorClass(),
                            variant != 14,
                            variant != 15,
                            variant == 16 ? 1 : 0,
                            "changed");
            var indexes = new ArrayList<>(table.indexes());
            if (variant == 17) indexes.remove(1);
            else indexes.set(1, changed);
            tables.set(0, withIndexes(table, indexes));
            assertFalse("variant " + variant, PgSchemaCheck.compare(schema, tables).isEmpty());
        }
        var indexes = new ArrayList<>(table.indexes());
        indexes.add(
                new ActualIndex(
                        "extra",
                        index.column(),
                        false,
                        false,
                        "btree",
                        1,
                        1,
                        true,
                        true,
                        true,
                        true,
                        false,
                        false,
                        false,
                        index.operatorClass(),
                        true,
                        true,
                        0,
                        "extra"));
        tables.set(0, withIndexes(table, indexes));
        assertDifference(schema, tables, "unexpected indexes");
    }

    private static ActualTable withColumns(ActualTable table, List<ActualColumn> columns) {
        return new ActualTable(
                table.name(),
                table.kind(),
                table.persistence(),
                table.inherited(),
                table.rowSecurity(),
                columns,
                table.indexes(),
                table.otherConstraints());
    }

    private static ActualTable withIndexes(ActualTable table, List<ActualIndex> indexes) {
        return new ActualTable(
                table.name(),
                table.kind(),
                table.persistence(),
                table.inherited(),
                table.rowSecurity(),
                table.columns(),
                indexes,
                table.otherConstraints());
    }

    private static void assertDifference(
            SourceSchema schema, List<ActualTable> actual, String text) {
        assertTrue(
                PgSchemaCheck.compare(schema, actual).toString(),
                PgSchemaCheck.compare(schema, actual).stream().anyMatch(d -> d.contains(text)));
    }
}
