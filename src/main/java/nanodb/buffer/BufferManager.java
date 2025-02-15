package nanodb.buffer;

import nanodb.exceptions.BufferAbortException;
import nanodb.file.Block;
import nanodb.file.FileManager;
import nanodb.log.LogManager;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Predicate;

import static nanodb.constants.NanoDBConstants.MAX_WAIT_TIME;

public class BufferManager {
    private final Buffer[] buffers;
    private int available;

    public BufferManager(FileManager fm, LogManager lm, int numBuffers) {
        buffers = new Buffer[numBuffers];
        available = numBuffers;
        Arrays.fill(buffers, new Buffer(fm, lm));
    }

    public synchronized int available() {
        return available;
    }

    public synchronized void flushAll(int txnNum) {
        Arrays.stream(buffers)
                .filter(buffer -> buffer.modifyingTxn() == txnNum)
                .forEach(Buffer::flush);
    }

    public synchronized void unpin(Buffer buffer) {
        buffer.unpin();
        if (!buffer.isPinned()) {
            available++;
            notifyAll();
        }
    }

    public synchronized Buffer pin(Block blk) {
        try {
            long timestamp = System.currentTimeMillis();
            Optional<Buffer> buffer = tryToPin(blk);

            while (buffer.isEmpty() && !waitingTooLong(timestamp)) {
                wait(MAX_WAIT_TIME);
                buffer = tryToPin(blk);
            }

            if (buffer.isEmpty()) throw new BufferAbortException();

            return buffer.get();
        } catch (InterruptedException ex) {
            throw new BufferAbortException();
        }
    }

    private boolean waitingTooLong(long startTime) {
        return System.currentTimeMillis() - startTime > MAX_WAIT_TIME;
    }

    private Optional<Buffer> tryToPin(Block blk) {
        Optional<Buffer> buffer = findExistingBuffer(blk);
        if (buffer.isEmpty()) {
            buffer = chooseUnpinnedBuffer();
            if (buffer.isEmpty()) return Optional.empty();

            buffer.get().assignToBlock(blk);
        }

        if (!buffer.get().isPinned()) available--;

        buffer.get().pin();

        return buffer;
    }

    private Optional<Buffer> findExistingBuffer(Block blk) {
        return findBuffer(buffer -> blk.equals(buffer.block()));
    }

    private Optional<Buffer> chooseUnpinnedBuffer() {
        return findBuffer(buffer -> !buffer.isPinned());
    }

    private Optional<Buffer> findBuffer(Predicate<Buffer> condition) {
        return Arrays.stream(buffers)
                .filter(condition)
                .findFirst();
    }
}
