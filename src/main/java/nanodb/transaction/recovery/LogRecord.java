package nanodb.transaction.recovery;

import nanodb.file.Block;
import nanodb.file.Page;

public sealed interface LogRecord permits StartRecord, TransactionRecord, DataUpdateRecord, CheckPointRecord {
    byte[] toBytes();

    int op();

    static LogRecord fromBytes(byte[] rec) {
        Page p = new Page(rec);
        int op = p.getInt(0);

        return switch (OpCode.values()[op]) {
            case CHECKPOINT -> new CheckPointRecord(op);
            case START -> new StartRecord(op, p.getInt(Integer.BYTES));
            case COMMIT -> new CommitRecord(op, p.getInt(Integer.BYTES));
            case ROLLBACK -> new RollbackRecord(op, p.getInt(Integer.BYTES));
            case SETINT -> deserialize(p, SetIntRecord::new, (page, offset) -> p.getInt(offset));
            case SETSTRING -> deserialize(p, SetStringRecord::new, (page, offset) -> p.getString(offset));
        };
    }

    private static <T> LogRecord deserialize(Page p, RecordConstructor<T> constructor, DataUpdateRecord.DataReader<T> dataReader) {
        int op = p.getInt(0);
        int txNum = p.getInt(Integer.BYTES);
        String fileName = p.getString(Integer.BYTES * 2);
        int fileNameLen = Page.maxLength(fileName.length());
        int blknum = p.getInt(Integer.BYTES * 2 + fileNameLen);
        Block blk = new Block(fileName, blknum);
        int offset = p.getInt(Integer.BYTES * 3 + fileNameLen);
        T value = dataReader.read(p, Integer.BYTES * 4 + fileNameLen);

        return constructor.create(op, txNum, blk, offset, value);
    }

    final class Utils {
        static byte[] createBasicRec(int op, int size) {
            byte[] rec = new byte[size];
            Page p = new Page(rec);
            p.putInt(0, op);

            return rec;
        }

        // Store op and txNum
        static byte[] createTxnRec(int op, int txNum, int size) {
            byte[] rec = createBasicRec(op, size);
            Page p = new Page(rec);
            p.putInt(Integer.BYTES, txNum);

            return rec;
        }
    }

    @FunctionalInterface
    interface DataReader<T> {
        T read(Page p, int pos);
    }

    @FunctionalInterface
    interface RecordConstructor<T> {
        LogRecord create(int op, int txNum, Block blk, int offset, T value);
    }
}

record StartRecord(int op, int txNum) implements LogRecord {

    @Override
    public byte[] toBytes() {
        return Utils.createTxnRec(op, txNum, Integer.BYTES * 2);
    }
}

record CheckPointRecord(int op) implements LogRecord {

    @Override
    public byte[] toBytes() {
        return Utils.createBasicRec(op, Integer.BYTES);
    }
}

sealed interface TransactionRecord extends LogRecord permits CommitRecord, RollbackRecord {
    int txNum();

    default byte[] toBytes() {
        return Utils.createTxnRec(op(), txNum(), Integer.BYTES * 2);
    }
}

record CommitRecord(int op, int txNum) implements TransactionRecord { }

record RollbackRecord(int op, int txNum) implements TransactionRecord { }

