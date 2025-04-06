package nanodb.transaction;

import nanodb.buffer.Buffer;
import nanodb.buffer.BufferManager;
import nanodb.file.Block;
import nanodb.file.FileManager;
import nanodb.file.Page;
import nanodb.log.LogManager;
import nanodb.transaction.concurrency.ConcurrencyManager;
import nanodb.transaction.recovery.*;

import java.util.function.Consumer;
import java.util.function.Supplier;

public class Transaction {
    private static int nextTxnNum = 0;
    private static final int END_OF_FILE = -1;
    private final RecoveryManager rm;
    private final ConcurrencyManager cm;
    private final BufferManager bm;
    private final FileManager fm;
    private final LogProcessor lp;
    private final int txnNum;
    private final BufferList buffers;

    public Transaction(FileManager fm, LogManager lm, LogProcessor lp, BufferManager bm) {
        this.fm = fm;
        this.bm = bm;
        this.lp = lp;
        txnNum = nextTxnNumber();
        rm = new RecoveryManager(this, txnNum, lm, bm, lp);
        cm = new ConcurrencyManager();
        buffers = new BufferList(bm);
    }

    public void commit() {
        rm.commit();
        cm.release();
        buffers.unpinAll();

        System.out.println("Transaction "+txnNum+" committed");
    }

    public void rollback() {
        rm.rollback();
        cm.release();
        buffers.unpinAll();

        System.out.println("Transaction "+txnNum+" rolled back");
    }

    public void recover() {
        bm.flushAll(txnNum);
        rm.recover();
    }

    public void unpin(Block blk) {
        buffers.unpin(blk);
    }

    public void pin(Block blk) {
        buffers.pin(blk);
    }

    public int getInt(Block blk, int offset) {
        cm.sLock(blk);
        Buffer buffer = buffers.getBuffer(blk);

        return buffer.contents().getInt(offset);
    }

    public String getString(Block blk, int offset) {
        cm.sLock(blk);
        Buffer buffer = buffers.getBuffer(blk);

        return buffer.contents().getString(offset);
    }

    private void setValue(Block blk, boolean okToLog, Supplier<DataUpdateRecord> dataRecSupplier, Consumer<Page> pageWriter) {
        cm.xLock(blk);
        Buffer buffer = buffers.getBuffer(blk);
        int lsn = -1;
        if (okToLog) lsn = lp.writeToLog(dataRecSupplier.get());

        Page p = buffer.contents();
        pageWriter.accept(p);
        buffer.setModified(txnNum, lsn);
    }

    public void setInt(Block blk, int offset, int val, boolean okToLog) {
        Supplier<DataUpdateRecord> recordSupplier = () -> DataUpdateRecord.createIntRecord(OpCode.SETSTRING.code, txnNum, blk, offset, val);
        Consumer<Page> pageWriter = (page) -> page.putInt(val, offset);

        setValue(blk, okToLog, recordSupplier, pageWriter);
    }

    public void setString(Block blk, int offset, String val, boolean okToLog) {
        Supplier<DataUpdateRecord> recordSupplier = () -> DataUpdateRecord.createStringRecord(OpCode.SETSTRING.code, txnNum, blk, offset, val);
        Consumer<Page> pageWriter = (page) -> page.putString(offset, val);

        setValue(blk, okToLog, recordSupplier, pageWriter);
    }

    public int size(String filename) {
        Block blk = new Block(filename, END_OF_FILE);
        cm.sLock(blk);

        return fm.length(filename);
    }

    public Block append(String filename) {
        // It's a trick to get a xLock on a dummy block so that we don't have to take lock on
        // entire file.
        // If 2 transactions tries to append to a same file, it's not possible because
        // there will be a xLock on Block with blk num 'END_OF_FILE'
        Block blk = new Block(filename, END_OF_FILE);
        cm.xLock(blk);

        return fm.append(filename);
    }

    public int availableBuffers() {
        return bm.available();
    }

    private static synchronized int nextTxnNumber() {
        nextTxnNum++;
        System.out.println("New transaction: "+nextTxnNum);

        return nextTxnNum;
    }
}
