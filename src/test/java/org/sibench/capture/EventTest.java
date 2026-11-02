package org.sibench.capture;

import static org.junit.Assert.*;

import com.google.gson.JsonParser;

import org.junit.Test;

import java.util.List;

public class EventTest {
    @Test
    public void rejectsNonJsonSyntaxAndNonObjectRecords() {
        for (String line : List.of(
                "{event:\"x\"}", "{'event':'x'}", "/* comment */ {\"event\":\"x\"}",
                "", "null", "nul", "[1,2]", "{\"event\":\"x\",", "{\"a\":1} {\"b\":2}",
                "{\"n\":007}", "{\"n\":NaN}", "{\"n\":Infinity}",
                "{\"a\":01x}", "{\"keys\":[{\"value\":NaN}]}", "{\"value\":\"a\nb\"}")) {
            assertThrows(line, com.google.gson.JsonParseException.class, () -> Event.parse(line));
        }
    }

    @Test
    public void preservesObjectFieldsAndCachedIdentities() {
        for (String line :
                List.of(
                        "{\"event\":\"scan_rescan\",\"pid\":7,\"keys\":[{\"attribute\":1,\"value\":\"a\\u00e9\\\"b\"}],\"n\":12345678901234,\"f\":1.5e3,\"x\":null,\"t\":true}",
                        "{\"event\":\"a\",\"pid\":1,\"event\":\"b\"}",
                        "{\"pid\":\"12\",\"row\":{\"c\":[1,2,{\"d\":{}}]}}",
                        "{}",
                        "  {\"event\":\"x\",\"pid\":3}  ")) {
            Event event = Event.parse(line);
            assertEquals(JsonParser.parseString(line), event.json());
            var whole = Event.of(JsonParser.parseString(line).getAsJsonObject());
            assertEquals(whole.has("event"), event.has("event"));
            if (whole.has("event")) assertEquals(whole.type(), event.type());
            if (whole.has("pid")) assertEquals(whole.pid(), event.pid());
        }
    }

    @Test
    public void preservesJsonNumbersAndInt64Boundaries() {
        for (String number : List.of("0", "7", "-12", "123456789012345678", "9223372036854775807", "-0", "1.5", "1e3", "-9223372036854775808", "99999999999999999999")) {
            String line = "{\"event\":\"x\",\"n\":" + number + "}";
            var parsed = Event.parse(line).get("n").getAsJsonPrimitive();
            assertTrue(number, parsed.isNumber());
            assertEquals(number, parsed.toString());
            assertEquals(number, parsed.getAsString());
        }
        assertEquals(Long.MAX_VALUE, Event.parse("{\"n\":9223372036854775807}").get("n").getAsLong());
        assertEquals(Long.MIN_VALUE, Event.parse("{\"n\":-9223372036854775808}").get("n").getAsLong());
        assertEquals(-12, Event.parse("{\"n\":-12}").get("n").getAsInt());
        assertEquals(1500, Event.parse("{\"n\":1.5e3}").get("n").getAsDouble(), 0);
        assertEquals(new java.math.BigInteger("99999999999999999999"),
                Event.parse("{\"n\":99999999999999999999}").get("n").getAsBigInteger());
    }
}
