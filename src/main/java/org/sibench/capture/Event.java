package org.sibench.capture;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.HashMap;
import java.util.Map;

/**
 * One backend trace event: its top-level fields by name in a hash map. The compiler reads a few
 * fields of every event many times on its single ordered thread, where Gson's tree-ordered member
 * lookup was a third of the time; parser threads build this form ({@link ParsedTrace}). Nested
 * values (row images, scan keys) stay Gson elements.
 *
 * <p>Field names are the interned strings, as the compiler's literals are, so a lookup matches by
 * identity without reading the name; the event type and backend pid every event is asked for are
 * read once, on the parsing thread.
 */
public final class Event {
    /** Interned field names; a trace has a few dozen, so the map stays small. */
    private static final java.util.concurrent.ConcurrentHashMap<String, String> NAMES =
            new java.util.concurrent.ConcurrentHashMap<>();
    private static final int MAX_NAMES = 1 << 12;

    private final Map<String, JsonElement> fields;
    /** The event type and pid, or null and 0 when not readable as such (see {@link #type}). */
    private final String type;
    private final int pid;
    private final boolean hasPid;

    private Event(Map<String, JsonElement> fields) {
        this.fields = fields;
        String type = null;
        int pid = 0;
        boolean hasPid = false;
        try {
            JsonElement value = fields.get("event");
            if (value != null) type = name(value.getAsString());
        } catch (RuntimeException unreadable) {
            // type() fails the way reading the field does.
        }
        try {
            JsonElement value = fields.get("pid");
            if (value != null) {
                pid = value.getAsInt();
                hasPid = true;
            }
        } catch (RuntimeException unreadable) {
            // pid() fails the way reading the field does.
        }
        this.type = type;
        this.pid = pid;
        this.hasPid = hasPid;
    }

    public static Event of(JsonObject object) {
        var fields = new HashMap<String, JsonElement>(object.size() * 2);
        for (var entry : object.entrySet()) fields.put(name(entry.getKey()), entry.getValue());
        return new Event(fields);
    }

    private static String name(String name) {
        String known = NAMES.get(name);
        if (known != null) return known;
        if (NAMES.size() >= MAX_NAMES) return name;
        known = name.intern();
        NAMES.putIfAbsent(known, known);
        return known;
    }

    /** The {@code event} field as a string, as {@code get("event").getAsString()}. */
    public String type() {
        return type != null ? type : fields.get("event").getAsString();
    }

    /** The {@code pid} field as an int, as {@code get("pid").getAsInt()}. */
    public int pid() {
        return hasPid ? pid : fields.get("pid").getAsInt();
    }

    /**
     * One trace line, as {@code of(JsonParser.parseString(line).getAsJsonObject())} gives it,
     * without building the top-level object first: its fields go straight into the map (a later
     * duplicate replaces an earlier one, as in a JsonObject). A line this does not read is parsed
     * that way instead, so it fails exactly as it did.
     */
    public static Event parse(String line) {
        try {
            var reader = new com.google.gson.stream.JsonReader(new java.io.StringReader(line));
            reader.setStrictness(com.google.gson.Strictness.LENIENT);
            if (reader.peek() == com.google.gson.stream.JsonToken.BEGIN_OBJECT) {
                var fields = new HashMap<String, JsonElement>(64);
                reader.beginObject();
                while (reader.hasNext()) {
                    String name = name(reader.nextName());
                    fields.put(
                            name,
                            reader.peek() == com.google.gson.stream.JsonToken.NUMBER
                                    ? number(reader.nextString())
                                    : com.google.gson.internal.bind.TypeAdapters.JSON_ELEMENT.read(reader));
                }
                reader.endObject();
                if (reader.peek() == com.google.gson.stream.JsonToken.END_DOCUMENT) return new Event(fields);
            }
        } catch (java.io.IOException | RuntimeException | StackOverflowError unread) {
            // Parsed again below, failing as it always did.
        }
        return of(JsonParser.parseString(line).getAsJsonObject());
    }

    /**
     * A top-level number. Gson keeps a number as its text and parses it again on every read; the
     * compiler reads the same few counters of every event, so a canonical integer is parsed once
     * here. Any other text stays as Gson keeps it, so it reads and prints as before.
     */
    private static JsonElement number(String text) {
        int length = text.length();
        if (length > 0 && length < 19) {
            boolean canonical = true;
            for (int i = 0; i < length && canonical; i++) {
                char c = text.charAt(i);
                canonical = c >= '0' && c <= '9' || c == '-' && i == 0 && length > 1;
            }
            canonical &= !(text.charAt(text.charAt(0) == '-' ? 1 : 0) == '0' && length > (text.charAt(0) == '-' ? 2 : 1));
            if (canonical && !text.equals("-0")) return new com.google.gson.JsonPrimitive(Long.parseLong(text));
        }
        return new com.google.gson.JsonPrimitive(new com.google.gson.internal.LazilyParsedNumber(text));
    }

    /** The field, or null when the event has none, as {@link JsonObject#get}. */
    public JsonElement get(String name) {
        return fields.get(name);
    }

    public boolean has(String name) {
        return fields.containsKey(name);
    }

    public JsonArray getAsJsonArray(String name) {
        return (JsonArray) fields.get(name);
    }

    public JsonObject getAsJsonObject(String name) {
        return (JsonObject) fields.get(name);
    }

    /** The event as a Gson object again, for messages and reports. */
    public JsonObject json() {
        var object = new JsonObject();
        fields.forEach(object::add);
        return object;
    }

    @Override
    public String toString() {
        return json().toString();
    }
}
