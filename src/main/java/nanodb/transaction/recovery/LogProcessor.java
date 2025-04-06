package nanodb.transaction.recovery;

import nanodb.log.LogManager;
import nanodb.transaction.Transaction;

import java.util.ArrayList;
import java.util.List;

public class LogProcessor {
    private final LogManager logManager;

    public LogProcessor(LogManager logManager) {
        this.logManager = logManager;
    }

    public int writeToLog(LogRecord record) {
        return logManager.append(record.toBytes());
    }

    public void flush(int lsn) {
        logManager.flush(lsn);
    }

    public List<LogRecord> readLogs() {
        List<LogRecord> records = new ArrayList<>();

        logManager.iterator().forEachRemaining(rec -> records.add(LogRecord.fromBytes(rec)));

        return records;
    }

    public void undo(DataUpdateRecord record, Transaction tx) {
        switch (record) {
            case SetIntRecord r -> {
                tx.pin(r.blk());
                tx.setInt(r.blk(), r.offset(), r.value(), true);
                tx.unpin(r.blk());
            }

            case SetStringRecord r -> {
                tx.pin(r.blk());
                tx.setString(r.blk(), r.offset(), r.value(), true);
                tx.unpin(r.blk());
            }
        }
    }
}
