package nanodb.record;

import java.util.*;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;

/**
 Schema represents a record in a given table.
 It contains the name, type of each fields in a record.
 It also holds the length of the field if the field type is varchar.

 @author Manoj Balaraj
 */
public class Schema {
    private final Map<String, FieldInfo> info = new HashMap<>();

    public void addField(String name, int type, int len) {
        info.put(name, new FieldInfo(type, len));
    }

    public void addIntField(String name) {
        addField(name, INTEGER, 0);
    }

    public void addStringField(String name, int len) {
        addField(name, VARCHAR, len);
    }

    public void add(String name, Schema schema) {
        addField(name, schema.type(name), schema.length(name));
    }

    public void addAll(Schema schema) {
        for (String name: schema.fields())
            add(name, schema);
    }

    public Collection<String> fields() {
        return info.keySet();
    }

    public boolean hasField(String name) {
        return info.containsKey(name);
    }

    public int type(String name) {
        return info.get(name).type();
    }

    public int length(String name) {
        return info.get(name).length();
    }

    record FieldInfo(int type, int length) { }
}
