package nanodb.transaction.concurrency;

import nanodb.constants.NanoDBConstants;
import nanodb.exceptions.LockAbortException;
import nanodb.file.Block;

import java.util.HashMap;
import java.util.Map;

public class LockTable {
    private Map<Block, Integer> locks = new HashMap<>();

    public synchronized void sLock(Block blk) {
        try {
            long timestamp = System.currentTimeMillis();
            while (hasXLock(blk) && !waitingTooLong(timestamp))
                wait(NanoDBConstants.MAX_WAIT_TIME);

            if (hasXLock(blk)) throw new LockAbortException();

            int val = getLockVal(blk);
            locks.put(blk, val + 1);
        } catch (InterruptedException ex) {
            throw new LockAbortException();
        }
    }

    public synchronized void xLock(Block blk) {
        try {
            long timestamp = System.currentTimeMillis();

            while (hasOtherSharedLocks(blk) && !waitingTooLong(timestamp))
                wait(NanoDBConstants.MAX_WAIT_TIME);

            if (hasOtherSharedLocks(blk)) throw new LockAbortException();

            locks.put(blk, -1);
        } catch (InterruptedException ex) {
            throw new LockAbortException();
        }
    }

    public synchronized void unlock(Block blk) {
        int val = getLockVal(blk);

        if (val > 1) locks.put(blk, val - 1);
        else {
            locks.remove(blk);
            notifyAll();
        }
    }

    private boolean hasXLock(Block blk) {
        return getLockVal(blk) < 0;
    }

    private boolean hasOtherSharedLocks(Block blk) {
        return getLockVal(blk) > 1;
    }

    private boolean waitingTooLong(long startTime) {
        return System.currentTimeMillis() - startTime > NanoDBConstants.MAX_WAIT_TIME;
    }

    private int getLockVal(Block blk) {
        Integer val = locks.get(blk);

        return (val == null) ? 0 : val;
    }

}
