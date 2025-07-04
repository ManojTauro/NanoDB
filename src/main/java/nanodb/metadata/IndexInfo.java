package nanodb.metadata;

import nanodb.record.Layout;
import nanodb.record.Schema;
import nanodb.transaction.Transaction;

import static java.sql.Types.INTEGER;

public class IndexInfo {
        private String idxname, fldname;
        private Transaction tx;
        private Schema tblSchema;
        private final Layout layout;
        private StatInfo si;

        public IndexInfo(String idxname, String fldname, Schema tblSchema, Transaction tx, StatInfo si) {
            this.idxname = idxname;
            this.fldname = fldname;
            this.tx = tx;
            this.layout = createLayout();
            this.si = si;
        }

        public Index open() {
            return new HashIndex(tx, idxname, layout);
//          return new BTreeIndex(tx, idxname, idxLayout);
        }

        public int blocksAccessed() {
            int rpb = tx.blockSize() / layout.slotSize();
            int numblocks = si.recordsOutput() / rpb;
            return HashIndex.searchCost(numblocks, rpb);
//          return BTreeIndex.searchCost(numblocks, rpb);
        }

        public int recordsOutput() {
            return si.recordsOutput() / si.distinctValues(fldname);
        }

        public int distinctValues(String fname) {
            return fldname.equals(fname) ? 1 : si.distinctValues(fldname);
        }

        private Layout createLayout() {
            Schema schema = new Schema();
            schema.addIntField("block");
            schema.addIntField("id");

            if (schema.type(fldname) == INTEGER)
                schema.addIntField("dataval");
            else {
                int fldlen = schema.length(fldname);
                schema.addStringField("dataval", fldlen);
            }
            return new Layout(schema);
        }
}
