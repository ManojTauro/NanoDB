package nanodb.transaction.recovery;

import nanodb.file.Block;
import nanodb.file.Page;

public sealed interface DataUpdateRecord extends LogRecord permits SetIntRecord, SetStringRecord {
    int txNum();

    Block blk();

    int offset();

    default byte[] serialize(int op, int valueLen, DataWriter writer) {
        int fileNamePos = Integer.BYTES * 2;
        int blockPos = fileNamePos + Page.maxLength(blk().filename().length());
        int offsetPos = blockPos + Integer.BYTES;
        int valPos = offsetPos + Integer.BYTES;
        int recordLen = valPos + valueLen;

        byte[] rec = Utils.createTxnRec(op, txNum(), recordLen);
        Page p = new Page(rec);

        p.putString(fileNamePos, blk().filename());
        p.putInt(blockPos, blk().blknum());
        p.putInt(offsetPos, offset());

        writer.write(p, valPos);

        return rec;
    }

    static DataUpdateRecord createStringRecord(int op, int txNum, Block blk, int offset, String value) {
        return new SetStringRecord(op, txNum, blk, offset, value);
    }

    static DataUpdateRecord createIntRecord(int op, int txNum, Block blk, int offset, int value) {
        return new SetIntRecord(op, txNum, blk, offset, value);
    }

    @FunctionalInterface
    interface DataWriter {
        void write(Page p, int offset);
    }
}

record SetIntRecord(int op, int txNum, Block blk, int offset, int value) implements DataUpdateRecord {

    @Override
    public byte[] toBytes() {
        DataWriter writer = (page, offset) -> page.putInt(offset, value);
        return serialize(op, Integer.BYTES, writer);
    }
}

record SetStringRecord(int op, int txNum, Block blk, int offset, String value) implements DataUpdateRecord {

    @Override
    public byte[] toBytes() {
        DataWriter writer = (page, offset) -> page.putString(offset, value);
        int valLen = Page.maxLength(value.length());

        return serialize(op, valLen, writer);
    }
}