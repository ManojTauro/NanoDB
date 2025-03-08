package nanodb.transaction.recovery;

import nanodb.buffer.BufferManager;
import nanodb.log.LogManager;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RecoveryManager {
    private LogManager lm;
    private LogProcessor lp;
    private BufferManager bm;
    private Transaction tx;
    private int txNum;

    public RecoveryManager(Transaction tx, int txNum, LogManager lm, BufferManager bm, LogProcessor lp) {
        this.txNum = txNum;
        this.tx = tx;
        this.lm = lm;
        this.bm = bm;
        lp.writeToLog(new StartRecord(OpCode.START.code, txNum));
    }
    
    public void commit() {
        bm.flushAll(txNum);
        int lsn = lp.writeToLog(new CommitRecord(OpCode.COMMIT.code, txNum));
        lp.flush(lsn);
    }

    /**
        Rollback current transaction
     */
    private void rollback() {
        for (LogRecord record: lp.readLogs()) {
            switch (record) {
                case StartRecord startRecord when startRecord.txNum() == txNum -> {
                    return;
                }
                case DataUpdateRecord updateRecord when updateRecord.txNum() == txNum -> lp.undo(updateRecord, tx);
                default -> {}
            }
        }
    }

    private void recover() {
        List<Integer> finishedTxs = new ArrayList<>();

        for (LogRecord record: lp.readLogs()) {
            switch (record) {
                case CheckPointRecord ignored -> {
                    return;
                }
                case TransactionRecord tr -> finishedTxs.add(tr.txNum());
                case DataUpdateRecord updateRecord -> {
                    if (!finishedTxs.contains(updateRecord.txNum())) lp.undo(updateRecord, tx);
                }
                default -> {}
            }
        }
    }
}
