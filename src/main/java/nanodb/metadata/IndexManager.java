package nanodb.metadata;

import nanodb.record.Layout;
import nanodb.record.Schema;
import nanodb.transaction.Transaction;

import java.util.HashMap;
import java.util.Map;

import static nanodb.constants.NanoDBConstants.MAX_TABLE_AND_FIELD_NAME_LENGTH;

public class IndexManager {
    private Layout layout;
    private TableManager tableManager;
    private StatManager statManager;

    public IndexManager(boolean isNew, TableManager tableManager, StatManager statManager, Transaction tx) {
        if (isNew) {
            Schema sch = new Schema();
            sch.addStringField("indexName", MAX_TABLE_AND_FIELD_NAME_LENGTH);
            sch.addStringField("tableName", MAX_TABLE_AND_FIELD_NAME_LENGTH);
            sch.addStringField("fieldName", MAX_TABLE_AND_FIELD_NAME_LENGTH);
            tableManager.createTable("indexcatalog", sch, tx);
        }
        this.tableManager = tableManager;
        this.statManager = statManager;
        layout = tableManager.getLayout("indexcatalog", tx);
    }

    public void createIndex(String idxname, String tableName, String fldname,Transaction tx) {
        TableScan ts = new TableScan(tx, "indexcatalog", layout);
        ts.insert();
        ts.setString("indexname", idxname);
        ts.setString("tablename", tableName);
        ts.setString("fieldname", fldname);
        ts.close();
    }

    public Map<String,IndexInfo> getIndexInfo(String tableName, Transaction tx) {
        Map<String, IndexInfo> result = new HashMap<>();

        TableScan ts = new TableScan(tx, "indexcatalog", layout);
        while (ts.next())
            if (ts.getString("tablename").equals(tableName)) {
                String idxname = ts.getString("indexname");
                String fldname = ts.getString("fieldname");
                Layout tableLayout = tableManager.getLayout(tableName, tx);
                StatInfo statInfo = statManager.getStatInfo(tableName, tableLayout, tx);
                IndexInfo indexInfo = new IndexInfo(idxname, fldname, tableLayout.schema(),tx, statInfo);

                result.put(fldname, indexInfo);
            }

        ts.close();

        return result;
    }
}
