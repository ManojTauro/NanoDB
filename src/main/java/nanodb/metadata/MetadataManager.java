package nanodb.metadata;

import nanodb.record.Layout;
import nanodb.record.Schema;
import nanodb.transaction.Transaction;

import java.util.Map;

public class MetadataManager {
    private static TableManager tableManager;
    private static ViewManager viewManager;
    private static StatManager statManager;
    private static IndexManager indexManager;

    public MetadataManager(boolean isNew, Transaction tx) {
        tableManager = new TableManager(isNew, tx);
        viewManager = new ViewManager(isNew, tableManager, tx);
        statManager = new StatManager(tableManager, tx);
        indexManager = new IndexManager(isNew, tableManager, statManager, tx);
    }

    public void createTable(String tableName, Schema sch, Transaction tx) {
        tableManager.createTable(tableName, sch, tx);
    }

    public Layout getLayout(String tableName, Transaction tx) {
        return tableManager.getLayout(tableName, tx);
    }

    public void createView(String viewname, String viewdef,
                           Transaction tx) {
        viewManager.createView(viewname, viewdef, tx);
    }

    public String getViewDef(String viewname, Transaction tx) {
        return viewManager.getViewDef(viewname, tx);
    }

    public void createIndex(String idxname, String tableName, String fldname, Transaction tx) {
        indexManager.createIndex(idxname, tableName, fldname, tx);
    }

    public Map<String, IndexInfo> getIndexInfo(String tableName, Transaction tx) {
        return indexManager.getIndexInfo(tableName, tx);
    }

    public StatInfo getStatInfo(String tableName, Layout layout, Transaction tx) {
        return statManager.getStatInfo(tableName, layout, tx);
    }
}
