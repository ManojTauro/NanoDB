package nanodb.record;

import nanodb.file.Page;

import java.util.HashMap;
import java.util.Map;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;

/**
 * Layout represents a physical organization of a Record/Schema in the database.
 * Given the Schema, it calculates exactly how many bytes required to store a Record.
 *
 * @author Manoj Balaraj
 */
public class Layout {
    private final Schema schema;
    private final Map<String, Integer> offsets;
    private final int slotsize;

    public Layout(Schema schema) {
        this.schema = schema;
        offsets = new HashMap<>();

        // First 4 bytes are used as an identifier to represent
        // whether this slot is empty or not.
        // 0 -> Empty, 1 -> Not Empty
        int pos = Integer.BYTES;

        for (String name: schema.fields()) {
            offsets.put(name, pos);
            pos += lengthInBytes(name);
        }

        // total Record size
        slotsize = pos;
    }

    public Schema schema() {
        return schema;
    }

    public int offset(String name) {
        return offsets.get(name);
    }

    public int slotSize() {
        return slotsize;
    }

    private int lengthInBytes(String name) {
        if (schema.type(name) == INTEGER) return Integer.BYTES;
        return Page.maxLength(schema.length(name));
    }
}
