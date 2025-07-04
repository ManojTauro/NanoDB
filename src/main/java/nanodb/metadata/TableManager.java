package nanodb.metadata;

import nanodb.record.Layout;
import nanodb.record.Schema;
import nanodb.transaction.Transaction;

import java.util.HashMap;
import java.util.Map;

import static nanodb.constants.NanoDBConstants.MAX_TABLE_AND_FIELD_NAME_LENGTH;

public class TableManager {
    private static final String TABLE_CATALOG_TABLE_NAME = "tablecatalog";
    private static final String FIELD_CATALOG_TABLE_NAME = "fieldcatalog";
    private Layout tableCatLayout, fieldCatLayout;

    public TableManager(boolean isNew, Transaction tx) {
        Schema tableCatSchema = getTableCatSchema();
        Schema fieldCatSchema = getFieldCatSchema();

        if (isNew) {
            createTable(TABLE_CATALOG_TABLE_NAME, tableCatSchema, tx);
            createTable(FIELD_CATALOG_TABLE_NAME, fieldCatSchema, tx);
        }
    }

    public void createTable(String tableName, Schema schema, Transaction tx) {
        Layout layout = new Layout(schema);

        TableScan tsTableCat = new TableScan(tx, TABLE_CATALOG_TABLE_NAME, tableCatLayout);
        tsTableCat.insert();
        tsTableCat.setStrig("tablename", tableName);
        tsTableCat.setInt("slotsize", layout.slotSize());
        tsTableCat.close();

        TableScan tsFieldCat = new TableScan(tx, FIELD_CATALOG_TABLE_NAME, fieldCatLayout);
        for (String fieldName: schema.fields()) {
            tsFieldCat.insert();
            tsFieldCat.setString("tablename", tableName);
            tsFieldCat.setString("fieldname", fieldName);
            tsFieldCat.setInt("type", schema.type(fieldName));
            tsFieldCat.setInt("length", schema.length(fieldName));
            tsFieldCat.setInt("offset", layout.offset(fieldName));
        }
        tsFieldCat.close();
    }

    public Layout getLayout(String tableName, Transaction tx) {
        int size = -1;
        TableScan tsTableCat = new TableScan(tx, TABLE_CATALOG_TABLE_NAME, tableCatLayout);
        while (tsTableCat.next()) {
            if (tableName.equals(tsTableCat.getString("tablename"))) {
                size = tsTableCat.getInt("slotsize");
                break;
            }
        }
        tsTableCat.close();

        Schema schema = new Schema();
        Map<String, Integer> offsets = new HashMap<>();
        TableScan tsFieldCat = new TableScan(tx, FIELD_CATALOG_TABLE_NAME, fieldCatLayout);

        while (tsFieldCat.next()) {
            if (tableName.equals(tsFieldCat.getString("tablename"))) {
                String fieldName = tsFieldCat.getString("fieldname");
                int fieldType = tsFieldCat.getInt("type");
                int fieldLength = tsFieldCat.getInt("length");
                int offset = tsFieldCat.getInt("offset");
                offsets.put(fieldName, offset);
                schema.addField(fieldName, fieldType, fieldLength);
            }
        }
        tsFieldCat.close();

        return new Layout(schema, offsets, size);
    }

    private Schema getFieldCatSchema() {
        Schema fieldCatSchema = new Schema();
        fieldCatSchema.addStringField("tablename", MAX_TABLE_AND_FIELD_NAME_LENGTH);
        fieldCatSchema.addStringField("fieldname", MAX_TABLE_AND_FIELD_NAME_LENGTH);
        fieldCatSchema.addIntField("type");
        fieldCatSchema.addIntField("length");
        fieldCatSchema.addIntField("offset");
        fieldCatLayout = new Layout(fieldCatSchema);
        return fieldCatSchema;
    }

    private Schema getTableCatSchema() {
        Schema tableCatSchema = new Schema();
        tableCatSchema.addStringField("tablename", MAX_TABLE_AND_FIELD_NAME_LENGTH);
        tableCatSchema.addIntField("slotsize");
        tableCatLayout = new Layout(tableCatSchema);
        return tableCatSchema;
    }
}
