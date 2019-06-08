package simpledb;

import java.util.NoSuchElementException;

public class BTreeReverseScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    private boolean isOpen = false;
    private final TransactionId tid;
    private String tableName;
    private String tableAlias;
    private transient DbFileIterator it;
    private TupleDesc td;
    private IndexPredicate ipred;

    public BTreeReverseScan(TransactionId tid, int tableid, String tableAlias, IndexPredicate ipred) {
        this.tid = tid;
        this.ipred = ipred;
        reset(tableid, tableAlias);
    }

    public String getTableAlias() {
        return tableAlias;
    }

    public String getTableName() {
        return tableName;
    }

    public void reset(int tableId, String tableAlias) {
        isOpen = false;
        tableName = Database.getCatalog().getTableName(tableId);
        this.tableAlias = tableAlias;
        if (ipred != null)
            it = ((BTreeFile) Database.getCatalog().getDatabaseFile(tableId)).reverseIndexIterator(tid, ipred);
        else
            it = ((BTreeFile) Database.getCatalog().getDatabaseFile(tableId)).reverseIterator(tid);
        td = Database.getCatalog().getTupleDesc(tableId);
        Database.getCatalog().getDatabaseFile(tableId);
        String[] newNames = new String[td.numFields()];
        Type[] newTypes = new Type[td.numFields()];
        for (int i = 0; i < td.numFields(); i++) {
            String name = td.getFieldName(i);
            Type t = td.getFieldType(i);

            newNames[i] = tableAlias + "." + name;
            newTypes[i] = t;
        }
        td = new TupleDesc(newTypes, newNames);
    }

    public BTreeReverseScan(TransactionId tid, int tableid, IndexPredicate ipred) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid), ipred);
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        if (isOpen)
            throw new DbException("double open on one OpIterator.");
        isOpen = true;
        it.open();
    }

    @Override
    public boolean hasNext() throws TransactionAbortedException, DbException {
        if (!isOpen)
            throw new IllegalStateException("iterator is closed");
        return it.hasNext();
    }

    @Override
    public Tuple next() throws NoSuchElementException, TransactionAbortedException, DbException {
        if (!isOpen)
            throw new IllegalStateException("iterator is closed");
        return it.next();
    }

    @Override
    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        close();
        open();
    }

    @Override
    public TupleDesc getTupleDesc() {
        return td;
    }

    @Override
    public void close() {
        it.close();
        isOpen = false;
    }
}
