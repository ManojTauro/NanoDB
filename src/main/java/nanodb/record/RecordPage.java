package nanodb.record;

import nanodb.constants.NanoDBConstants;
import nanodb.file.Block;
import nanodb.transaction.Transaction;

import static java.sql.Types.INTEGER;

/**
 * Class to manage storing and retrieving of records in a given block
 */
public class RecordPage {
    public static final int EMPTY = 0, USED = 1;
    private final Transaction tx;
    private final Block blk;
    private final Layout layout;

    public RecordPage(Transaction tx, Block blk, Layout layout) {
        this.tx = tx;
        this.blk = blk;
        this.layout = layout;

        tx.pin(blk);
    }

    public int getInt(int slot, String name) {
        int fieldPos = getFieldPos(slot, name);
        return tx.getInt(blk, fieldPos);
    }

    public String getString(int slot, String name) {
        int fieldPos = getFieldPos(slot, name);
        return tx.getString(blk, fieldPos);
    }

    public void setInt(int slot, String name, int val) {
        int fieldPos = getFieldPos(slot, name);
        tx.setInt(blk, fieldPos, val, true);
    }

    public void setString(int slot, String name, String val) {
        int fieldPos = getFieldPos(slot, name);
        tx.setString(blk, fieldPos, val, true);
    }

    public void delete(int slot) {
        setFlag(slot, EMPTY, true);
    }

    public void format() {
        int slot = 0;

        while (isValidSlot(slot)) {
            setFlag(slot, EMPTY, false);
            Schema schema = layout.schema();
            for (String name: schema.fields()) {
                int fieldPos = getFieldPos(slot, name);
                if (schema.type(name) == INTEGER) tx.setInt(blk, fieldPos, 0, false);
                else tx.setString(blk, fieldPos, "", false);
            }

            slot++;
        }
    }

    public int nextAfter(int slot) {
        return searchAfter(slot, USED);
    }

    public int insertAfter(int slot) {
        int newSlot = searchAfter(slot, EMPTY);

        if (newSlot >= 0) setFlag(newSlot, USED, false);

        return newSlot;
    }

    public Block block() {
        return blk;
    }

    private int searchAfter(int slot, int flag) {
        slot++;

        while (isValidSlot(slot)) {
            if (tx.getInt(blk, offset(slot)) == flag) return slot;
            slot++;
        }

        return -1;
    }

    private boolean isValidSlot(int slot) {
        return offset(slot + 1) <= NanoDBConstants.PAGE_SIZE;
    }

    private void setFlag(int slot, int flag, boolean okToLog) {
        tx.setInt(blk, offset(slot), flag, okToLog);
    }

    private int getFieldPos(int slot, String name) {
       return offset(slot) + layout.offset(name);
    }

    private int offset(int slot) {
        return slot * layout.slotSize();
    }
}
