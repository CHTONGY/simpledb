package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;
    private int pagesNum;
    private int tableId;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
        this.tableId = this.getId();
        int pageSize = BufferPool.getPageSize();
        this.pagesNum = (int) Math.ceil(((double) this.f.length()) / pageSize);
        Database.getCatalog().addTable(this);
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            RandomAccessFile raf = new RandomAccessFile(this.f, "r");
            int pageSize = BufferPool.getPageSize();
            byte[] curContent = new byte[pageSize];
            // find the start point of current file
            raf.seek(pid.pageNumber() * pageSize);
            raf.read(curContent, 0, pageSize);
            // for table id, each file is a table, so for all pages, they have the same tableid
            // but for pgno, each page is different
            HeapPageId newHeapPageId = new HeapPageId(pid.getTableId(), pid.pageNumber());

            HeapPage heapPage = new HeapPage(newHeapPageId, curContent);
            raf.close();

            return heapPage;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return this.pagesNum;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        HeapFileIterator hfIter = new HeapFileIterator(this.tableId, this.pagesNum, tid);
        return hfIter;
    }

    private class HeapFileIterator implements DbFileIterator {
        private TransactionId tid;
        private int pagesNum;
        private int tableId;

        private int curIndex;
        private Iterator<Tuple> curIter;

        private boolean isOpen;

        public HeapFileIterator(int tableId, int pagesNum, TransactionId tid) {
            this.pagesNum = pagesNum;
            this.tid = tid;
            this.tableId = tableId;
            this.curIndex = 0;
            this.isOpen = false;
            HeapPage curPage = getPage(this.curIndex);
            this.curIter = curPage.iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.isOpen = true;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(!this.isOpen) {
                return false;
            }
            return this.curIter.hasNext() || (this.curIndex < this.pagesNum - 1);
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(!this.isOpen) {
                throw new NoSuchElementException("iterator not open yet");
            }
            if(this.curIter.hasNext()) {
                Tuple t = this.curIter.next();
//                // after iterate the last tuple in the last page, we should unpin the last page
//                if(this.curIndex == this.pagesNum-1 && !this.curIter.hasNext()) {
//                    Database.getBufferPool().unpinPage(new HeapPageId(this.tableId, this.curIndex));
//                }
                return t;
            }
            if(this.curIndex < this.pagesNum - 1) {
//                Database.getBufferPool().unpinPage(new HeapPageId(this.tableId, this.curIndex++));
                HeapPage nxtPage = getPage(++this.curIndex);
                this.curIter = nxtPage.iterator();
                return curIter.next();
            }
            throw new NoSuchElementException("no more element for next");
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.curIndex = 0;
            HeapPage firstPage = getPage(this.curIndex);
            this.curIter = firstPage.iterator();
        }

        @Override
        public void close() {
            this.isOpen = false;
        }

        private HeapPage getPage(int pageNo) {
            HeapPage curPage = null;
            try {
                curPage = (HeapPage) Database.getBufferPool().getPage(this.tid, new HeapPageId(this.tableId, pageNo), Permissions.READ_ONLY);
            } catch (TransactionAbortedException e) {
                e.printStackTrace();
            } catch (DbException e) {
                e.printStackTrace();
            }
            return curPage;
        }
    }
}




