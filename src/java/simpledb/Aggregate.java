package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator[] children;
    private int aField;
    private int gField;
    private Aggregator.Op aop;

    private Aggregator aggregator;
    private DbIterator aggregatorIt;
    private TupleDesc td;
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
        this.aField = afield;
        this.gField = gfield;
        this.aop = aop;
        this.children = new DbIterator[] {child};

        Type aType = child.getTupleDesc().getFieldType(afield);
        Type[] tdType = new Type[]{aType};
        String[] tdAttr = new String[]{aop.name()};
        Type gType = null;
        if(gfield != -1) {
            gType = child.getTupleDesc().getFieldType(gfield);
            tdType = new Type[]{gType, aType};
            tdAttr = new String[]{gType.name(), aop.name()};
        }

//        td = new TupleDesc(tdType, tdAttr);
        td = new TupleDesc(tdType);

        if(aType.equals(Type.INT_TYPE)) {
            aggregator = new IntegerAggregator(Math.abs(gField), gType, afield, aop);
        } else {
            aggregator = new StringAggregator(Math.abs(gField), gType, afield, aop);
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
	    return this.gField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
	    return children[0].getTupleDesc().getFieldName(this.gField);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
	    return this.aField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
	    return children[0].getTupleDesc().getFieldName(this.aField);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
	    return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	    // some code goes here
        super.open();
        for(DbIterator child : children) {
            child.open();
            while (child.hasNext()) {
                aggregator.mergeTupleIntoGroup(child.next());
            }
        }
        aggregatorIt = aggregator.iterator();
        aggregatorIt.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
        if(aggregatorIt.hasNext()) {
            return aggregatorIt.next();
        }
	    return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	    // some code goes here
        aggregatorIt.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
	    return td;
    }

    public void close() {
	    // some code goes here
        super.close();
        for(DbIterator child : children) {
            child.close();
        }
        aggregatorIt.close();
    }

    @Override
    public DbIterator[] getChildren() {
	// some code goes here
	    return this.children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
	// some code goes here
        this.children = children;
    }
    
}
