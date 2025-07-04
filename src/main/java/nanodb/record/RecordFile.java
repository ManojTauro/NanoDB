package nanodb.record;

import nanodb.file.Block;
import nanodb.transaction.Transaction;

import static java.sql.Types.INTEGER;

/**
 * This class is used to manager records in a given file.
 *
 * @author Manoj Balaraj
 */
public class RecordFile {
    private final Transaction tx;
    private final Layout layout;
    private RecordPage rp;
    private final String fileName;
    private int currentSlot;

    public RecordFile(Transaction tx, String tableName, Layout layout) {
        this.tx = tx;
        this.fileName = tableName + ".tbl";
        this.layout = layout;

        if (tx.size(fileName) == 0) moveToNewBlock();
        else moveToBlock(0);
    }

    public void close() {
        if (rp != null) tx.unpin(rp.block());
    }

    public void beforeFirst() {
        moveToBlock(0);
    }

    public boolean next() {
        currentSlot = rp.nextAfter(currentSlot);
        while (currentSlot < 0) {
            if (atLastBlock())
                return false;
            moveToBlock(rp.block().blknum() + 1);
            currentSlot = rp.nextAfter(currentSlot);
        }
        return true;
    }

    public int getInt(String name) {
        return rp.getInt(currentSlot, name);
    }

    public String getString(String name) {
        return rp.getString(currentSlot, name);
    }

    public Constant getVal(String name) {
        if (layout.schema().type(name) == INTEGER)
            return new IntConstant(getInt(name));
        else
            return new StringConstant(getString(name));
    }

    public boolean hasField(String name) {
        return layout.schema().hasField(name);
    }

    public void setInt(String name, int val) {
        rp.setInt(currentSlot, name, val);
    }

    public void setString(String name, String val) {
        rp.setString(currentSlot, name, val);
    }

    public void setVal(String name, Constant val) {
        if (layout.schema().type(name) == INTEGER)
            setInt(name, (Integer)val.asJavaVal());
        else
            setString(name, (String)val.asJavaVal());
    }

    public void insert() {
        currentSlot = rp.insertAfter(currentSlot);
        while (currentSlot < 0) {
            if (atLastBlock())
                moveToNewBlock();
            else
                moveToBlock(rp.block().blknum()+1);
            currentSlot = rp.insertAfter(currentSlot);
        }
    }
    
    public void delete() {
        rp.delete(currentSlot);
    }
    
    public void moveToRid(RID rid) {
        close();
        Block blk = new Block(fileName, rid.blkNum());
        rp = new RecordPage(tx, blk, layout);
        currentSlot = rid.slot();
    }
    
    private boolean atLastBlock() {
        return rp.block().blknum() == tx.size(fileName) - 1;
    }

    private void moveToBlock(int blkNum) {
        close();
        Block blk = new Block(fileName, blkNum);
        rp = new RecordPage(tx, blk, layout);
        currentSlot = -1;
    }

    private void moveToNewBlock() {
        close();
        Block blk = tx.append(fileName);
        rp = new RecordPage(tx, blk, layout);
        rp.format();
        currentSlot = -1;
    }
}
