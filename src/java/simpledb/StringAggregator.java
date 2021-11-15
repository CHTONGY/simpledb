package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbField;
    private int aField;
    private Map<Field, Integer> countMap;
    private TupleDesc td;

    private boolean hasGrouping;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if(!what.equals(Op.COUNT)) {
            throw new IllegalArgumentException("only support COUNT aggregate");
        }
        this.gbField = gbfield;
        this.aField = afield;
        this.countMap = new HashMap<>();
        this.hasGrouping = gbfieldtype != null;

        if(hasGrouping) {
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE},
                    new String[]{gbfieldtype.name(), what.name()});
        } else {
            td = new TupleDesc(new Type[]{Type.INT_TYPE},
                    new String[]{what.name()});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field key = tup.getField(this.gbField);
        if(!hasGrouping) {
            key = new StringField("", 0);
        }
        countMap.put(key, countMap.getOrDefault(key, 0) + 1);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        return new StringAggregatorIterator();
    }


    private class StringAggregatorIterator implements DbIterator {
        private boolean isOpen;
        private Iterator<Field> keyIterator;

        public StringAggregatorIterator() {
            keyIterator = countMap.keySet().iterator();
        }

        /**
         * Opens the iterator. This must be called before any of the other methods.
         *
         * @throws DbException when there are problems opening/accessing the database.
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            isOpen = true;
            keyIterator = countMap.keySet().iterator();
        }

        /**
         * Returns true if the iterator has more tuples.
         *
         * @return true f the iterator has more tuples.
         * @throws IllegalStateException If the iterator has not been opened
         */
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(!isOpen) {
                return false;
            }
            return keyIterator.hasNext();
        }

        /**
         * Returns the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return the next tuple in the iteration.
         * @throws NoSuchElementException if there are no more tuples.
         * @throws IllegalStateException  If the iterator has not been opened
         */
        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            Field key = keyIterator.next();
            int count = countMap.get(key);
            Tuple t = new Tuple(td);

            if(hasGrouping) {
                t.setField(0, key);
                t.setField(1, new IntField(count));
            } else {
                t.setField(0, new IntField(count));
            }
            return t;
        }

        /**
         * Resets the iterator to the start.
         *
         * @throws DbException           when rewind is unsupported.
         * @throws IllegalStateException If the iterator has not been opened
         */
        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            keyIterator = countMap.keySet().iterator();
        }

        /**
         * Returns the TupleDesc associated with this DbIterator.
         *
         * @return the TupleDesc associated with this DbIterator.
         */
        @Override
        public TupleDesc getTupleDesc() {
            return td;
        }

        /**
         * Closes the iterator. When the iterator is closed, calling next(),
         * hasNext(), or rewind() should fail by throwing IllegalStateException.
         */
        @Override
        public void close() {
            isOpen = false;
            keyIterator = null;
        }
    }
}
