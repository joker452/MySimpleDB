package simpledb;

import javax.xml.crypto.Data;
import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    private final TransactionId tid;
    private int tableid;
    private String tableAlias;
    private TupleDesc tdWithPrefix = null;
    private boolean isOpen = false;
    private transient DbFileIterator it = null;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid        The transaction this scan is running as a part of.
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tid = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias;
    }

    /**
     * @return return the table name of the table the operator scans. This should
     * be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableid);
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        // some code goes here
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     *
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        // isOpen = false?
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        tdWithPrefix = null;
        it = null;
        isOpen = false;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        it = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
        it.open();
        isOpen = true;
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        if (tdWithPrefix == null) {
            TupleDesc td = Database.getCatalog().getTupleDesc(tableid);
            String prefix = (tableAlias == null) ? "null" : tableAlias;
            int numFields = td.numFields();
            Type[] typeAr = new Type[numFields];
            String[] fieldAr = new String[numFields];
            for (int i = 0; i < numFields; ++i) {
                typeAr[i] = td.getFieldType(i);
                fieldAr[i] = prefix + "." + td.getFieldName(i);
            }
            tdWithPrefix = new TupleDesc(typeAr, fieldAr);
        }
        return tdWithPrefix;
    }


    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (isOpen)
            return it.hasNext();
        throw new IllegalStateException("SeqScan iterator is not open!");
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if (isOpen)
            return it.next();
        throw new IllegalStateException("SeqScan iterator is not open!");
    }

    public void close() {
        // some code goes here
        it.close();
        isOpen = false;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        if (isOpen)
            it.rewind();
        else
            throw new IllegalStateException("SeqScan iterator is not open!");
    }
}
