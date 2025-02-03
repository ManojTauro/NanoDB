package nanodb.file;

import nanodb.constants.NanoDBConstants;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

public class FileManager {
    private final File dbDirectory;
    private final int blocksize;
    private final boolean isNew;
    private final Map<String, RandomAccessFile> openFiles = new HashMap<>();

    public FileManager(File dbDirectory) {
        this.dbDirectory = Objects.requireNonNull(dbDirectory, "Database directory cannot be null");
        this.blocksize = NanoDBConstants.PAGE_SIZE;
        isNew = !dbDirectory.exists();

        if(!dbDirectory.exists() && !dbDirectory.mkdirs())
            throw new IllegalStateException("Failed to create database directory "+dbDirectory);

        cleanupTempFiles();
    }

    public synchronized void read(Block blk, Page p) {
        try {
            RandomAccessFile f = getFile(blk.filename());
            f.seek((long) blk.blknum() * blocksize);
            f.getChannel().read(p.contents());
        } catch (IOException ex) {
            throw new RuntimeException("Cannot read block "+blk);
        }
    }

    public synchronized void write(Block blk, Page p) {
        try {
            RandomAccessFile f = getFile(blk.filename());
            f.seek((long) blk.blknum() * blocksize);
            f.getChannel().write(p.contents());
        } catch (IOException ex) {
            throw new RuntimeException("Cannot write block "+blk);
        }
    }

    public synchronized Block append(String filename) {
        int newBlknum = size(filename);
        Block newBlk = new Block(filename, newBlknum);
        byte[] b = new byte[blocksize];

        try {
            RandomAccessFile f = getFile(newBlk.filename());
            f.seek((long) newBlknum * blocksize);
            f.write(b);
        } catch (IOException ex) {
            throw new RuntimeException("Cannot append block "+newBlk);
        }

        return newBlk;
    }

    private int size(String filename) {
        try {
            RandomAccessFile f = getFile(filename);

            return (int) (f.length() / blocksize);
        } catch (IOException e) {
            throw new RuntimeException("cannot access "+ filename);
        }
    }

    private RandomAccessFile getFile(String filename) throws IOException {
        if (openFiles.containsKey(filename)) return openFiles.get(filename);

        File dbTable = new File(dbDirectory, filename);
        RandomAccessFile f = new RandomAccessFile(dbTable, "rws");
        openFiles.put(filename, f);

        return f;
    }

    private void cleanupTempFiles() {
        File[] tempFiles = dbDirectory.listFiles((dir, name) -> name.startsWith("temp"));
        if (tempFiles != null) {
            for (File file: tempFiles) {
                if (!file.delete()) System.out.println("Failed to delete file "+file.getName());
            }
        }
    }
}
