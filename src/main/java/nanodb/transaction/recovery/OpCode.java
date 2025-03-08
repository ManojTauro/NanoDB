package nanodb.transaction.recovery;

public enum OpCode {
    CHECKPOINT(1),
    START(2),
    COMMIT(3),
    ROLLBACK(4),
    SETINT(5),
    SETSTRING(6);

    public final int code;

    OpCode(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }
}
