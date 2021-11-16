package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator[] children;
    private TupleDesc td;

    private boolean hasScan;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
        this.tid = t;
        this.children = new DbIterator[]{child};
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        for(DbIterator child : children) {
            child.open();
        }
    }

    public void close() {
        // some code goes here
        super.close();
        for(DbIterator child : children) {
            child.close();
        }
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        for(DbIterator child : children) {
            child.rewind();
        }
        hasScan = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(!hasScan) {
            int count = 0;
            for (DbIterator child : children) {
                while (child.hasNext()) {
                    try {
                        Database.getBufferPool().deleteTuple(tid, child.next());
                        count++;
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            Tuple t = new Tuple(td);
            t.setField(0, new IntField(count));
            hasScan = true;
            return t;
        } else {
            return null;
        }
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        this.children = children;
    }

}
