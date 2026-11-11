package org.sibench.capture;

import static org.junit.Assert.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.schema.SourceSchema;

public class PixelsInputsTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test public void csvDistinguishesNullQuotedEmptyCommasAndEscapedQuotes() throws Exception {
        assertEquals(Arrays.asList("1", null, "", "a,b", "a\"b", null), PixelsInputs.csv("1,,\"\",\"a,b\",\"a\"\"b\","));
        for (String bad : List.of("\"unclosed", "a\"b", "\"a\"b"))
            assertThrows(IOException.class, () -> PixelsInputs.csv(bad));
    }

    @Test public void shardsPreservePostgresNullAndCharSemanticsAndAllRows() throws Exception {
        Path csv = temporary.newFolder().toPath(); HybenchTest.fixture(csv);
        Files.writeString(csv.resolve("customer.csv"), Files.readString(csv.resolve("customer.csv"))
                .replace(",2023-01-01,", ",2023-01-01 03:04:05.123,"));
        Files.writeString(csv.resolve("company.csv"), "2,\"\",\"a,b\",10,100000,123,Province,City,1,1,1,0,2023-01-01,2023-01-02 00:00:00\n");
        Files.writeString(csv.resolve("transfer.csv"),
                "10,2,1,50,salary ,2023-01-01 00:00:00,\n11,2,1,60,salary,2023-01-01 00:00:00,\n12,2,1,70,,2023-01-01 00:00:00,\n");
        var source = SourceSchema.hybench(HybenchTest.source());
        Path output = temporary.getRoot().toPath().resolve("pixels");
        var prepared = PixelsInputs.prepare(source, csv, output, 2);
        assertEquals(prepared, PixelsInputs.inspect(source, csv));
        assertEquals(3L, prepared.rows().get("transfer").longValue());
        try (var files = Files.list(output.resolve("transfer"))) { assertEquals(2, files.count()); }
        assertEquals(List.of("10|2|1|50|salary    |2023-01-01 00:00:00|\\N", "11|2|1|60|salary    |2023-01-01 00:00:00|\\N"),
                Files.readAllLines(output.resolve("transfer/part-000000.tbl")));
        assertTrue(Files.readString(output.resolve("company/part-000000.tbl")).startsWith("2||a,b|"));
        assertTrue(Files.readString(output.resolve("customer/part-000000.tbl")).contains("|2023-01-01|"));
        assertTrue(prepared.schema().text().contains("name:varchar(256)"));
        assertEquals("bigint", prepared.schema().keyTypes().get("sibench_marker"));
        Files.writeString(csv.resolve("company.csv"), Files.readString(csv.resolve("company.csv")).replace("a,b", "a|b"));
        assertThrows(IOException.class, () -> PixelsInputs.prepare(source, csv, temporary.getRoot().toPath().resolve("invalid"), 2));
    }
}
