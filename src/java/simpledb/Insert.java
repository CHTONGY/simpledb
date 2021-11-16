package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator[] children;
    private int tableId;
    private TupleDesc td;

    private boolean hasScan;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
        // some code goes here
//        if(!Database.getCatalog().getDatabaseFile(tableId).getTupleDesc().equals(child.getTupleDesc())) {
//            throw new DbException("TupleDesc of dbIterator differs from table into which we are to insert");
//        }
        this.tid = t;
        this.children = new DbIterator[]{child};
        this.tableId = tableId;
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
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(!hasScan) {
            int count = 0;
            for (DbIterator child : children) {
                while (child.hasNext()) {
                    Tuple t = child.next();
                    try {
                        Database.getBufferPool().insertTuple(this.tid, this.tableId, t);
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
