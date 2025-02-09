package nanodb.log;

import nanodb.constants.NanoDBConstants;
import nanodb.file.Block;
import nanodb.file.FileManager;
import nanodb.file.Page;

import java.util.Iterator;

public class LogManager {
    private final FileManager fileManager;
    private final String logFile;
    private final Page logPage;
    private Block currentBlk;
    private int latestLSN;
    private int lastSavedLSN;

    public LogManager(FileManager fileManager, String logfile) {
        this.fileManager = fileManager;
        this.logFile = logfile;

        byte[] b = new byte[NanoDBConstants.PAGE_SIZE];
        logPage = new Page(b);

        int logpageSize = fileManager.length(logfile);
        if (logpageSize == 0) currentBlk = appendNewBlk();
        else {
            currentBlk = new Block(logfile, logpageSize - 1);
            fileManager.read(currentBlk, logPage);
        }
    }

    /**
     * @param logRecord - Log record to be stored in the Page/Disc.
     * @return - Returns a latest Log Sequence Number
     */
    public synchronized int append(byte[] logRecord) {
        int boundary = logPage.getInt(0);
        int requiredBytes = logRecord.length + Integer.BYTES;

        if (boundary - requiredBytes < Integer.BYTES) {
            flush();
            currentBlk = appendNewBlk();
            boundary = logPage.getInt(0);
        }

        int recordPos = boundary - requiredBytes;
        logPage.putBytes(recordPos, logRecord);
        logPage.putInt(0, recordPos);
        latestLSN++;

        return latestLSN;
    }

    private Block appendNewBlk() {
        Block blk = fileManager.append(logFile);
        logPage.putInt(0, NanoDBConstants.PAGE_SIZE);
        fileManager.write(blk, logPage);

        return blk;
    }

    private void flush() {
        fileManager.write(currentBlk, logPage);
        lastSavedLSN = latestLSN;
    }

    public void flush(int lsn) {
        if (lsn >= lastSavedLSN) flush();
    }

    public Iterator<byte[]> iterator() {
        flush();
        return new LogIterator(fileManager, currentBlk);
    }

}
