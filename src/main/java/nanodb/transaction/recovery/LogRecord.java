package nanodb.transaction.recovery;

import nanodb.file.Block;
import nanodb.file.Page;

import java.util.Arrays;

public sealed interface LogRecord permits TransactionRecord, DataUpdateRecord, CheckPointRecord {
    byte[] toBytes();
    int op();
}

record CheckPointRecord(int op) implements LogRecord {

    @Override
    public byte[] toBytes() {
        byte[] rec = new byte[Integer.BYTES];
        Page p = new Page(rec);
        p.putInt(0, op);

        return rec;
    }
}

sealed interface TransactionRecord extends LogRecord permits CommitRecord, RollbackRecord {
    int txnNum();

    default byte[] toBytes() {
        // Store op and txnNum
        byte[] rec = new byte[Integer.BYTES * 2];
        Page p = new Page(rec);
        p.putInt(0, op());
        p.putInt(Integer.BYTES, txnNum());

        return rec;
    }
}

record CommitRecord(int op, int txnNum) implements TransactionRecord {}

record RollbackRecord(int op, int txnNum) implements TransactionRecord {}

sealed interface DataUpdateRecord extends LogRecord permits SetIntRecord, SetStringRecord {
    int txnNum();
    Block blk();
    int offset();


    default byte[] toBytes(int op, int valueLen, DataWriter writer) {
        int fileNamePos = Integer.BYTES * 2;
        int blockPos = fileNamePos + Page.maxLength(blk().filename().length());
        int offsetPos = blockPos + Integer.BYTES;
        int valPos = offsetPos + Integer.BYTES;
        int recordLen = valPos + valueLen;

        byte[] rec = new byte[recordLen];
        Page p = new Page(rec);

        p.putInt(0, op);
        p.putInt(Integer.BYTES, txnNum());
        p.putString(fileNamePos, blk().filename());
        p.putInt(blockPos, blk().blknum());
        p.putInt(offsetPos, offset());

        writer.write(p, valPos);

        return rec;
    }

    @FunctionalInterface
    interface DataWriter {
        void write(Page p, int offset);
    }
}

record SetIntRecord(int op, int txnNum, Block blk, int offset, int value) implements DataUpdateRecord {
    public byte[] toBytes() {
        DataWriter writer = (page, offset) -> page.putInt(offset, value);
        return toBytes(op, Integer.BYTES, writer);
    }
}

record SetStringRecord(int op, int txnNum, Block blk, int offset, String value) implements DataUpdateRecord {

    @Override
    public byte[] toBytes() {
        DataWriter writer = (page, offset) -> page.putString(offset, value);
        int valLen = Page.maxLength(value.length());

        return toBytes(op, valLen, writer);
    }
}

