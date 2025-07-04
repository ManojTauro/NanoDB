package nanodb.metadata;

import nanodb.record.Layout;
import nanodb.record.Schema;
import nanodb.transaction.Transaction;

import static nanodb.constants.NanoDBConstants.MAX_TABLE_AND_FIELD_NAME_LENGTH;

public class ViewManager {
    private static final int MAX_VIEW_DEF_LENGTH = 100;
    TableManager tableManager;

    public ViewManager(boolean isNew, TableManager tableManager, Transaction tx) {
        this.tableManager = tableManager;

        if (isNew) {
            Schema schema = new Schema();
            schema.addStringField("viewname", MAX_TABLE_AND_FIELD_NAME_LENGTH);
            schema.addStringField("viewdef", MAX_VIEW_DEF_LENGTH);
            tableManager.createTable("viewcat", schema, tx);
        }
    }

    public void createView(String name, String def, Transaction tx) {
        Layout layout = tableManager.getLayout("viewcat", tx);
        TableScan ts = new TableScan(tx, "viewcat", layout);
        ts.insert();
        ts.setString("viewname", name);
        ts.setString("viewdef", def);
        ts.close();
    }

    public String getViewDef(String name, Transaction tx) {
        String result = null;

        Layout layout = tableManager.getLayout("viewcat", tx);
        TableScan ts = new TableScan(tx, "viewcat", layout);

        while (ts.next()) {
            if (name.equals(ts.getString("viewname"))) {
                result = ts.getString("viewdef");
                break;
            }
        }
        ts.close();

        return result;
    }
}
