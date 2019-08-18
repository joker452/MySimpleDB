package simpledb;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private final File f;
    private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    @Override
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    @Override
    public Page readPage(PageId pid) {
        // some code goes here
        int pageSize = BufferPool.getPageSize();
        HeapPageId id = (HeapPageId) pid;
        byte[] pageData = new byte[pageSize];
        try (RandomAccessFile file = new RandomAccessFile(f, "r")) {

            try {
                file.seek(pageSize * id.getPageNumber());
            } catch (IOException e1) {
                throw new IllegalArgumentException("Unable to seek to correct place in HeapFile");
            }
            int byteRead = file.read(pageData);
            if (byteRead == -1) {
                throw new IllegalArgumentException("Read past end of table");
            }
            if (byteRead < pageSize) {
                throw new IllegalArgumentException("Unable to read "
                        + BufferPool.getPageSize() + " bytes from HeapFile");
            }

            return new HeapPage(id, pageData);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    @Override
    public synchronized void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pageSize = BufferPool.getPageSize();
        HeapPageId id = (HeapPageId) page.getId();
        byte[] pageData = page.getPageData();
        try (RandomAccessFile file = new RandomAccessFile(f, "rw")) {
            try {
                file.seek(pageSize * id.getPageNumber());
            } catch (IOException e1) {
                throw new IllegalArgumentException("Unable to seek to correct place in HeapFile");
            }
            file.write(pageData);
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    @Override
    public synchronized ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int numPages = numPages();
        boolean done = false;
        HeapPage heapPage = null;
        ArrayList<Page> pages = new ArrayList<>();
        for (int i = 0; i < numPages; ++i) {
            heapPage = (HeapPage) Database.getBufferPool().getPage(tid,
                    new HeapPageId(getId(), i), Permissions.READ_ONLY);
            if (heapPage.getNumEmptySlots() > 0) {
                done = true;
                break;
            }
            /* early release lock */
            Database.getBufferPool().releasePage(tid, new HeapPageId(getId(), i));
        }
        if (!done) {
            heapPage = new HeapPage(new HeapPageId(getId(), numPages), HeapPage.createEmptyPageData());
        } else {
            heapPage = (HeapPage) Database.getBufferPool().getPage(tid,
                    heapPage.getId(), Permissions.READ_WRITE);
            pages.add(heapPage);
        }
        heapPage.insertTuple(t);
        if (!done)
            writePage(heapPage);
        return pages;
    }

    // see DbFile.java for javadocs
    /*
    add by Deng
    this method doesn't mark dirty bit
     */
    @Override
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int numPages = numPages();
        boolean done = false;
        HeapPage heapPage;
        ArrayList<Page> pages = new ArrayList<>();
        for (int i = 0; i < numPages; ++i) {
            heapPage = (HeapPage) Database.getBufferPool().getPage(tid,
                    new HeapPageId(getId(), i), Permissions.READ_WRITE);
            try {
                heapPage.deleteTuple(t);
                done = true;
                pages.add(heapPage);
                break;
            } catch (DbException e) {
                /* release unused page */
                Database.getBufferPool().releasePage(tid, new HeapPageId(getId(), i));
            }
        }

        if (done)
            return pages;
        else
            throw new DbException("tuple cannot be deleted or is not a member of the file");

    }

    // see DbFile.java for javadocs
    @Override
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid, this);
    }

}

class HeapFileIterator extends AbstractDbFileIterator {

    private final HeapFile f;
    private final TransactionId tid;
    private Iterator<Tuple> it;
    private int currentPage;

    public HeapFileIterator(TransactionId tid, HeapFile f) {
        this.tid = tid;
        this.f = f;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid,
                new HeapPageId(f.getId(), 0), Permissions.READ_ONLY);
        it = heapPage.iterator();
        currentPage = 0;
    }

    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        if (it != null) {
            if (it.hasNext()) {
                return it.next();
            } else {
                if (currentPage < f.numPages() - 1) {
                    HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid,
                            new HeapPageId(f.getId(), ++currentPage), Permissions.READ_ONLY);
                    it = heapPage.iterator();
                    return readNext();
                }
            }
        }
        return null;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid,
                new HeapPageId(f.getId(), 0), Permissions.READ_ONLY);
        it = heapPage.iterator();
        currentPage = 0;
    }

    @Override
    public void close() {
        super.close();
        it = null;
        currentPage = -1;
    }
}