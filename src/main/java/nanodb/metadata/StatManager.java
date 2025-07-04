package nanodb.metadata;

import nanodb.record.Layout;
import nanodb.transaction.Transaction;

import java.util.HashMap;
import java.util.Map;

public class StatManager {
    private final TableManager tableManager;
    private Map<String, StatInfo> tableStats;
    private int numCalls;

    public StatManager(TableManager tableManager, Transaction tx) {
        this.tableManager = tableManager;
        refreshStatistics(tx);
    }

    public synchronized StatInfo getStatInfo(String tableName, Layout layout, Transaction tx) {
        numCalls++;
        if (numCalls > 100) refreshStatistics(tx);

        StatInfo si = tableStats.get(tableName);
        if (si == null) {
            si = calcTableStats(tableName, layout, tx);
            tablestats.put(tableName, si);
        }

        return si;
    }

    private synchronized void refreshStatistics(Transaction tx) {
        tableStats = new HashMap<>();
        this.numCalls = 0;
        Layout tableCatLayout = tableManager.getLayout("tablecatalog", tx);
        TableScan tcat = new TableScan(tx, "tablecatalog", tableCatLayout);

        while (tcat.next()) {
            String tableName = tcat.getString("tablename");
            Layout layout = tableManager.getLayout(tableName, tx);
            StatInfo si = calcTableStats(tableName, layout, tx);
            tableStats.put(tableName, si);
        }

        tcat.close();
    }

    private synchronized StatInfo calcTableStats(String tableName, Layout layout, Transaction tx) {
        int numRecs = 0;
        int numblocks = 0;
        TableScan ts = new TableScan(tx, tableName, layout);
        while (ts.next()) {
            numRecs++;
            numblocks = ts.getRid().blockNumber() + 1;
        }
        ts.close();

        return new StatInfo(numblocks, numRecs);
    }
}
