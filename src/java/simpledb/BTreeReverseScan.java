package simpledb;

import java.util.NoSuchElementException;

public class BTreeReverseScan implements OpIterator {

    public BTreeReverseScan(TransactionId tid, int tableid, String tableAlias, IndexPredicate ipred) {

    }

    @Override
    public void open() throws DbException, TransactionAbortedException {

    }

    @Override
    public boolean hasNext() throws TransactionAbortedException, DbException {
        return true;
    }

    @Override
    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        return null;
    }

    @Override
    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
    }

    @Override
    public TupleDesc getTupleDesc() {
        return null;
    }

    @Override
    public void close() {

    }
}
