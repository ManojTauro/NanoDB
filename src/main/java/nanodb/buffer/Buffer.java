package nanodb.buffer;

import nanodb.file.Block;
import nanodb.file.FileManager;
import nanodb.file.Page;
import nanodb.log.LogManager;

public class Buffer {
    private final FileManager fileManager;
    private final LogManager logManager;
    private final Page contents;
    private Block blk;
    private int txnNum;
    private int lsn;
    private int pins;

    public Buffer(FileManager fileManager, LogManager logManager) {
        this.fileManager = fileManager;
        this.logManager = logManager;
        contents = new Page();
    }

    public Page contents() {
        return contents;
    }

    public Block block() {
        return blk;
    }

    public void setModified(int txnNum, int lsn) {
        this.txnNum = txnNum;
        if (lsn >= 0) this.lsn = lsn;
    }

    public boolean isPinned() {
        return pins > 0;
    }

    public int modifyingTxn() {
        return txnNum;
    }

    void assignToBlock(Block blk) {
        flush();
        this.blk = blk;
        fileManager.read(blk, contents);
        pins = 0;
    }

    void flush() {
        if (txnNum >= 0) {
            logManager.flush(lsn);
            fileManager.write(blk, contents);
            txnNum = -1;
        }
    }

    void pin() {
        pins++;
    }

    void unpin() {
        pins--;
    }
}
