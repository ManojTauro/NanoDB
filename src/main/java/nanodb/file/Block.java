package nanodb.file;

public record Block(
        String filename,
        int blknum
) { }
