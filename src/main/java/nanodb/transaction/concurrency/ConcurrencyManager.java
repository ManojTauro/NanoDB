package nanodb.transaction.concurrency;

import nanodb.file.Block;

import java.util.HashMap;
import java.util.Map;

public class ConcurrencyManager {
    private static final LockTable lockTable = new LockTable();
    private final Map<Block, String> locks = new HashMap<>();

    public void sLock(Block blk) {
        if (!locks.containsKey(blk)) {
            lockTable.sLock(blk);
            locks.put(blk, "S");
        }
    }

    public void xLock(Block blk) {
        if (!hasXLock(blk)) {
            sLock(blk);
            lockTable.xLock(blk);
            locks.put(blk, "X");
        }
    }

    public void release() {
        for (Block blk: locks.keySet()) lockTable.unlock(blk);

        locks.clear();
    }

    private boolean hasXLock(Block blk) {
        return locks.containsKey(blk) && "X".equals(locks.get(blk));
    }

}
