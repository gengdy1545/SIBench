package org.sibench.generation;

import static org.junit.Assert.*;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.format.*;
import org.junit.Test;

public class CanonicalParquetFooterTest {
    @Test public void canonicalizesEncodingOrderWithoutTouchingData() throws Exception {
        Path a = Files.createTempFile("sib-footer-a", ".parquet"), b = Files.createTempFile("sib-footer-b", ".parquet");
        try {
            Files.write(a, file(List.of(Encoding.RLE, Encoding.PLAIN, Encoding.RLE_DICTIONARY)));
            Files.write(b, file(List.of(Encoding.PLAIN, Encoding.RLE_DICTIONARY, Encoding.RLE)));
            assertFalse(java.util.Arrays.equals(Files.readAllBytes(a), Files.readAllBytes(b)));
            CanonicalParquetFooter.normalize(a); CanonicalParquetFooter.normalize(b);
            assertArrayEquals(Files.readAllBytes(a), Files.readAllBytes(b));
            byte[] once = Files.readAllBytes(a); CanonicalParquetFooter.normalize(a);
            assertArrayEquals(once, Files.readAllBytes(a));
        } finally { Files.deleteIfExists(a); Files.deleteIfExists(b); }
    }

    private byte[] file(List<Encoding> encodings) throws Exception {
        var column = new ColumnMetaData(Type.INT64, new ArrayList<>(encodings), List.of("id"), CompressionCodec.UNCOMPRESSED, 1, 8, 8, 4);
        var chunk = new ColumnChunk(4); chunk.setMeta_data(column);
        var group = new RowGroup(List.of(chunk), 8, 1);
        var root = new SchemaElement("table"); root.setNum_children(1);
        var field = new SchemaElement("id"); field.setType(Type.INT64); field.setRepetition_type(FieldRepetitionType.REQUIRED);
        var metadata = new FileMetaData(1, List.of(root, field), 1, List.of(group));
        ByteArrayOutputStream footer = new ByteArrayOutputStream(); Util.writeFileMetaData(metadata, footer);
        ByteArrayOutputStream output = new ByteArrayOutputStream(); output.write(new byte[]{'P','A','R','1',42});
        output.write(footer.toByteArray()); output.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(footer.size()).array());
        output.write(new byte[]{'P','A','R','1'});return output.toByteArray();
    }
}
