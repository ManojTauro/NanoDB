package nanodb.log;

import nanodb.file.FileManager;
import nanodb.file.Page;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;
import java.util.stream.Stream;

class LogManagerTest {
    File db = new File("db");
    private final LogManager logManager = new LogManager(new FileManager(db), "logtest");

    private final Path DB_DIRECTORY = Path.of("db");
    private final Path LOG_FILE = Path.of("logtest");

    @AfterEach
    void cleanUp() throws IOException {
        Files.deleteIfExists(LOG_FILE);

        if (Files.exists(DB_DIRECTORY)) {
            try (Stream<Path> walk = Files.walk(DB_DIRECTORY)) {
                walk.sorted(Comparator.reverseOrder())  // Delete files before directories
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (IOException e) {
                                throw new RuntimeException("Failed to delete " + path, e);
                            }
                        });
            }
        }
    }

    @Test
    void appendTest() {
        createLogRecords(1, 35);
        printLogRecords();
        createLogRecords(36, 70);
        logManager.flush(65);
        printLogRecords();
    }

    private void printLogRecords() {
        System.out.println("The log file has these records");

        Iterator<byte[]> it = logManager.iterator();

        while (it.hasNext()) {
            byte[] rec = it.next();
            Page p = new Page(rec);
            String s = p.getString(0);
            int inPos = Page.maxLength(s.length());
            int value = p.getInt(inPos);

            System.out.println("[" + s + ", " + value + "]");
        }
        System.out.println();
    }

    private void createLogRecords(int start, int end) {
        System.out.println("Creating records: ");
        for (int i = start; i <= end; i++) {
            byte[] rec = getLogRecord("record " + i, i + 100);
            int lsn = logManager.append(rec);
            System.out.println(lsn + " ");
        }

        System.out.println();
    }

    private byte[] getLogRecord(String s, int n) {
        int intPos = Page.maxLength(s.length());
        byte[] b = new byte[intPos + Integer.BYTES];
        Page p = new Page(b);
        p.putString(0, s);
        p.putInt(intPos, n);

        return b;
    }
}
