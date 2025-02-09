package nanodb.log;

import nanodb.constants.NanoDBConstants;
import nanodb.file.Block;
import nanodb.file.FileManager;
import nanodb.file.Page;

import java.util.Iterator;

public class LogIterator implements Iterator<byte[]> {
    private final FileManager fileManager;
    private Block blk;
    private final Page page;
    private int currentPos;

    public LogIterator(FileManager fileManager, Block blk) {
        this.fileManager = fileManager;
        this.blk = blk;
        byte[] b = new byte[NanoDBConstants.PAGE_SIZE];
        page = new Page(b);
        moveToBlock(blk);
    }

    @Override
    public boolean hasNext() {
        return (currentPos < NanoDBConstants.PAGE_SIZE) ||
                (blk.blknum() > 0);
    }

    @Override
    public byte[] next() {
        if (currentPos == NanoDBConstants.PAGE_SIZE) {
            blk = new Block(blk.filename(), blk.blknum() - 1);
            moveToBlock(blk);
        }

        byte[] rec = page.getBytes(currentPos);
        currentPos += Integer.BYTES + rec.length;

        return rec;
    }

    private void moveToBlock(Block blk) {
        fileManager.read(blk, page);
        int boundary = page.getInt(0);
        currentPos = boundary;
    }
}
