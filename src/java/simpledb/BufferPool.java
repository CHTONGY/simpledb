package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private class Frame {
        // frame id
        private int frameID;
        // page id
        private PageId pageId;
        // page
        private Page page;
        // pin_count
        private int pinCount;
        // as for dirty, we can use page.isDirty() to check
        // the file that contains this page. use when flush
        private DbFile file;


        public Frame() {
        }

        public Frame(int frameID) {
            this.frameID = frameID;
        }

        public Frame(int frameID, PageId pageId, Page page, int pinCount) {
            this.frameID = frameID;
            this.pageId = pageId;
            this.page = page;
            this.pinCount = pinCount;
        }

        public int getFrameID() {
            return frameID;
        }

        public void setFrameID(int frameID) {
            this.frameID = frameID;
        }

        public PageId getPageId() {
            return pageId;
        }

        public void setPageId(PageId pageId) {
            this.pageId = pageId;
        }

        public Page getPage() {
            return page;
        }

        public void setPage(Page page) {
            this.page = page;
        }

        public int getPinCount() {
            return pinCount;
        }

        public void setPinCount(int pinCount) {
            this.pinCount = pinCount;
        }

        public DbFile getFile() {
            return file;
        }

        public void setFile(DbFile file) {
            this.file = file;
        }

        public void pinCountAddOne() {
            this.pinCount++;
        }

        public void pinCountMinusOne() {
            // TODO: what if pinCount already equals to 0 before minus one?
            this.pinCount--;
        }
    }

    private int numPages;
//    private Map<PageId, Page> idPageMap;
    private Frame[] frames;
    private List<Integer> unpinnedFrames;
    private Map<PageId, Integer> pageIdFrameIdMap;
//    private Map<TransactionId, List<Integer>> transactionIdFrameIdsMap;
//    private ReadWriteLock rwLock;
//    private Lock rLock;
//    private Lock wLock;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.frames = new Frame[numPages];
        this.unpinnedFrames = new LinkedList<>();
        this.pageIdFrameIdMap = new HashMap<>();
//        this.transactionIdFrameIdsMap = new HashMap<>();
        for(int i = 0; i < numPages; i++) {
            // init Frame[] with empty frame object
            this.frames[i] = new Frame(i);
            // at first, each frame is unpinned
            this.unpinnedFrames.add(i);        }

    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
//        rLock.lock();
        //if contains page id
        if(this.pageIdFrameIdMap.containsKey(pid)) {
            int frameID = this.pageIdFrameIdMap.get(pid);

            // update pinCount
            frames[frameID].pinCountAddOne();
            return frames[frameID].getPage();
        }
        // page not in buffer pool
        int unpinnedFrameId = 0;
        try {
            unpinnedFrameId = this.unpinnedFrames.get(0);   // always get the first unpinned frame id
        } catch (IndexOutOfBoundsException e) {
            e.printStackTrace();
        }
        Frame frame = this.frames[unpinnedFrameId];

        Page oldPage = frame.getPage();
        if(oldPage != null) {
            evictPage();
        }
        // read from catalog
        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page p = file.readPage(pid);
        frame.pageId = pid;
        frame.page = p;
        frame.file = file;
        frame.pinCountAddOne();
        this.pageIdFrameIdMap.put(pid, unpinnedFrameId);
        this.unpinnedFrames.remove(0);    // dequeue from unpinned frames list
        //        rLock.unlock();
        return p;
    }


    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtyPages = file.insertTuple(tid, t);
        // TODO: what should I do with these dirty pages?
        for(Page dirtyPage : dirtyPages) {
//            setFramePageDirty(tid, dirtyPage.getId());
//            flushPage(dirtyPage.getId());
            flushPage(dirtyPage);
        }
    }

    private void setFramePageDirty(TransactionId tid, PageId pageId) {
        if(this.pageIdFrameIdMap.containsKey(pageId)) {
            int frameId = this.pageIdFrameIdMap.get(pageId);
            Frame frame = this.frames[frameId];
            frame.getPage().markDirty(true, tid);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for(PageId pageId : this.pageIdFrameIdMap.keySet()) {
            flushPage(pageId);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1

    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        // use pid to get table id -> find dbfile in catalog by tableid -> write dbfile
        if(this.pageIdFrameIdMap.containsKey(pid)) {
            int frameId = this.pageIdFrameIdMap.get(pid);
            Frame frame = this.frames[frameId];

            Page dirtyPage = frame.page;
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());

            // page is dirty, then flush and set not dirty
            if(dirtyPage.isDirty() != null) {
            file.writePage(dirtyPage);
            dirtyPage.markDirty(false, null);
            }
        }
    }

    private synchronized void flushPage(Page page) throws IOException {
            DbFile file = Database.getCatalog().getDatabaseFile(page.getId().getTableId());
            file.writePage(page);
            page.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2

    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        // evict with LRU policy
        int unpinnedFramesId = this.unpinnedFrames.get(0);
        Frame frame = this.frames[unpinnedFramesId];

        // if page is dirty, flush the page
        if(frame.getPage().isDirty() != null) {
            try {
                flushPage(this.frames[this.unpinnedFrames.get(0)].getPageId());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        this.pageIdFrameIdMap.remove(frame.pageId);

        frame.page = null;
        frame.pageId = null;
        frame.file = null;
    }

    /** unpin a page when requestor has fulfilled the page*/
    public void unpinPage(PageId pageId) {
        if(this.pageIdFrameIdMap.containsKey(pageId)) {
            int frameId = this.pageIdFrameIdMap.get(pageId);
            Frame frame = this.frames[frameId];
            frame.pinCountMinusOne();
            if(frame.getPinCount() == 0) {
                this.unpinnedFrames.add(frameId);
            }
        }
    }

    public void unpinPage(Page page) {
        unpinPage(page.getId());
    }

}
