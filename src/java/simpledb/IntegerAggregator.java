package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbField;
    private int aField;
    private Op what;

    private Map<Field, Field> maxMap;   // key: group field, value: aggregate field
    private Map<Field, Field> minMap;
    private Map<Field, Field> sumMap;
    private Map<Field, Integer> countMap;

    private boolean hasGrouping;

    private TupleDesc td;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbField = gbfield;
        this.aField = afield;
        this.what = what;
        this.maxMap = new HashMap<>();
        this.minMap = new HashMap<>();
        this.sumMap = new HashMap<>();
        this.countMap = new HashMap<>();
        this.hasGrouping = gbfieldtype != null;

        if(hasGrouping) {
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE},
                    new String[]{gbfieldtype.name(), what.toString()});
        } else {
            td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{what.toString()});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field key = tup.getField(this.gbField);
        if(!hasGrouping) {
            key = new IntField(0);
        }
        Field aF = tup.getField(this.aField);
        if(!maxMap.containsKey(key) || maxMap.get(key).compare(Predicate.Op.LESS_THAN, aF)) {
            maxMap.put(key, aF);
        }
        if(!minMap.containsKey(key) || minMap.get(key).compare(Predicate.Op.GREATER_THAN, aF)) {
            minMap.put(key, aF);
        }
        if(!sumMap.containsKey(key)) {
            sumMap.put(key, aF);
        } else {
            Field prevSum = sumMap.get(key);
            int newVal = ((IntField) prevSum).getValue() + ((IntField) aF).getValue();
            sumMap.put(key, new IntField(newVal));
        }
        countMap.put(key, countMap.getOrDefault(key, 0) + 1);
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        return new IntegerAggregatorIterator();
    }


    private class IntegerAggregatorIterator implements DbIterator {
        private boolean isOpen;
        private Map<Field, Field> m;
        private Iterator<Field> keyIterator;
        public IntegerAggregatorIterator() {
            switch (what) {
                case MIN:
                    m = minMap;
                    break;
                case MAX:
                    m = maxMap;
                    break;
                case SUM:
                case COUNT:
                case AVG:
                    m = sumMap;
                    break;
            }
            keyIterator = m.keySet().iterator();
        }

        /**
         * Opens the iterator. This must be called before any of the other methods.
         *
         * @throws DbException when there are problems opening/accessing the database.
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.isOpen = true;
            this.keyIterator = m.keySet().iterator();
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
            Tuple t = new Tuple(td);
            Field key = keyIterator.next();
            if(what.equals(Op.MIN) || what.equals(Op.MAX) || what.equals(Op.SUM)) {
                Field value = m.get(key);
                if(hasGrouping) {
                    t.setField(0, key);
                    t.setField(1, value);
                } else {
                    t.setField(0, value);
                }
            } else {
                IntField sum = (IntField) m.get(key);
                int count = countMap.get(key);
                if(what.equals(Op.COUNT)) {
                    if(hasGrouping) {
                        t.setField(0, key);
                        t.setField(1, new IntField(count));
                    } else {
                        t.setField(0, new IntField(count));
                    }
                } else {
                    int avg = sum.getValue() / count;
                    if(hasGrouping) {
                        t.setField(0, key);
                        t.setField(1, new IntField(avg));
                    } else {
                        t.setField(0, new IntField(avg));
                    }
                }
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
            keyIterator = m.keySet().iterator();
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
            this.isOpen = false;
            this.keyIterator = null;
        }
    }

}
