package nanodb.transaction;

import nanodb.buffer.Buffer;
import nanodb.buffer.BufferManager;
import nanodb.file.Block;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BufferList {
    private final Map<Block, Buffer> buffers = new HashMap<>();
    private final List<Block> pins = new ArrayList<>();
    private final BufferManager bm;

    public BufferList(BufferManager bm) {
        this.bm = bm;
    }

    Buffer getBuffer(Block blk) {
        return buffers.get(blk);
    }

    void pin(Block blk) {
        Buffer buff = bm.pin(blk);
        buffers.put(blk, buff);
        pins.add(blk);
    }

    void unpin(Block blk) {
        Buffer buff = buffers.get(blk);
        bm.unpin(buff);
        pins.remove(blk);

        if (!pins.contains(blk)) buffers.remove(blk);
    }

    void unpinAll() {
        pins.forEach(this::unpin);

        buffers.clear();
        pins.clear();
    }
}
