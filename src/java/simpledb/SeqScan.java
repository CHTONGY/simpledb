package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private int tableId;
    private HeapFile dbFile;
    private String tableAlias;

    private boolean isOpen; // for iterator
    private DbFileIterator innerIterator;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableId
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableId, String tableAlias) {
        // some code goes here
        this.tid = tid;
         this.tableId = tableId;
        this.dbFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        this.tableAlias = tableAlias;
        this.isOpen = false;
        this.innerIterator = this.dbFile.iterator(tid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(this.tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return this.tableAlias;
    }

    /**
     * Reset the tableId, and tableAlias of this operator.
     * @param tableId
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableId, String tableAlias) {
        // some code goes here
        this.tableId = tableId;
        this.tableAlias = tableAlias;
        this.dbFile = (HeapFile) Database.getCatalog().getDatabaseFile(this.tableId);
        this.isOpen = false;
        this.innerIterator.close();
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.isOpen = true;
        this.innerIterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc oriTd = dbFile.getTupleDesc();
        int fieldNum = oriTd.numFields();

        Type[] newTs = new Type[fieldNum];
        String[] newAs = new String[fieldNum];

        for(int i = 0; i < fieldNum; i++) {
            Type t = oriTd.getFieldType(i);
            String arr = oriTd.getFieldName(i);
            if(tableAlias != null || tableAlias != "") {
                arr = tableAlias + "." + arr;
            }
            newTs[i] = t;
            newAs[i] = arr;
        }

        return new TupleDesc(newTs, newAs);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(!this.isOpen) {
            throw new IllegalStateException("not open yet");
        }
        return this.innerIterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if(!this.isOpen) {
            throw new IllegalStateException("not open yet");
        }
        if(!this.innerIterator.hasNext()) {
            throw new NoSuchElementException("do not have next element");
        }
        return this.innerIterator.next();
    }

    public void close() {
        // some code goes here
        this.isOpen = false;
        this.innerIterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        if(!this.isOpen) {
            throw new IllegalStateException("not open yet");
        }
        this.innerIterator.rewind();
    }
}
