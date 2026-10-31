package org.sibench.capture;

import static org.junit.Assert.*;

import com.google.gson.JsonParser;

import org.junit.Test;

import java.util.List;

public class EventTest {
    /** The streamed parse gives the object, type and pid that parsing the whole line gives. */
    @Test
    public void parsesAsTheWholeLineParses() {
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

    /** Numbers read and print as Gson's own: canonical integers and every other spelling. */
    @Test
    public void numbersReadAndPrintAsGsonKeepsThem() {
        for (String number : List.of("0", "7", "-12", "123456789012345678", "9223372036854775807", "-0", "007", "1.5", "1e3", "-9223372036854775808", "99999999999999999999")) {
            String line = "{\"event\":\"x\",\"n\":" + number + "}";
            var whole = JsonParser.parseString(line).getAsJsonObject().get("n").getAsJsonPrimitive();
            var parsed = Event.parse(line).get("n").getAsJsonPrimitive();
            assertEquals(number, whole, parsed);
            assertEquals(number, whole.toString(), parsed.toString());
            assertEquals(number, whole.getAsString(), parsed.getAsString());
            assertEquals(number, whole.getAsDouble(), parsed.getAsDouble(), 0);
            if (!number.contains(".") && !number.contains("e") && number.length() < 19) {
                assertEquals(number, whole.getAsLong(), parsed.getAsLong());
                assertEquals(number, whole.getAsInt(), parsed.getAsInt());
            }
            assertEquals(number, JsonParser.parseString(line), Event.parse(line).json());
            assertEquals(number, JsonParser.parseString(line).toString(), Event.parse(line).json().toString());
        }
    }

    /** A line that does not parse fails with the exception parsing the whole line throws. */
    @Test
    public void failsAsTheWholeLineFails() {
        for (String line : List.of("", "{\"event\":\"x\",", "{\"a\":1} {\"b\":2}", "[1,2]", "{\"a\":01x}", "nul")) {
            Class<?> expected = null;
            try {
                JsonParser.parseString(line).getAsJsonObject();
            } catch (RuntimeException error) {
                expected = error.getClass();
            }
            Class<?> actual = null;
            try {
                Event.parse(line);
            } catch (RuntimeException error) {
                actual = error.getClass();
            }
            assertEquals(line, expected, actual);
        }
    }
}
