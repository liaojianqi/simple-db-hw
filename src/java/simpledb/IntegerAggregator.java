package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;
import java.util.NoSuchElementException;
import java.lang.IllegalStateException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    int gbfield;
    Type gbfieldtype;
    int afield;
    Op what;

    HashMap<Field, Integer> hm; // group value -> aggregate value
    HashMap<Field, Integer> hmHelp; // group value -> aggregate value

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
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        this.hm = new HashMap<>();
        this.hmHelp = new HashMap<>();
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
        IntField iNewF = (IntField)tup.getField(this.afield);

        Field gbF = Aggregator.NO_GROUPING_FIELD;
        if (gbfield != Aggregator.NO_GROUPING) {
            gbF = tup.getField(gbfield);
        }
        Integer v = hm.get(gbF);
        if (v == null) {
            if (this.what == Op.COUNT) {
                hm.put(gbF, 1);
                return ;
            }
            hm.put(gbF, iNewF.getValue());
            if (this.what == Op.AVG) {
                hmHelp.put(gbF, 1);
            }
            return;
        }
        switch (this.what) {
        case COUNT:
            hm.put(gbF, v+1);
            break;
        case MIN:
            hm.put(gbF, Math.min(v, iNewF.getValue()));
            break;
        case MAX:
            hm.put(gbF, Math.max(v, iNewF.getValue()));
            break;
        case SUM:
            hm.put(gbF, v+iNewF.getValue());
            break;
        case AVG:
            hm.put(gbF, v+iNewF.getValue());
            // helper
            Integer hv = hmHelp.get(gbF);
            if (hv == null) {
                hmHelp.put(gbF, 0);
                hv = hmHelp.get(gbF);
            }
            hmHelp.put(gbF, hv+1);
            break;
        default:
            break;
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here

        // 1. hm is last result
        Vector<Tuple> res = new Vector<Tuple>();
        TupleDesc td;
        if (this.gbfieldtype != null) {
            td = new TupleDesc(new Type[]{this.gbfieldtype, Type.INT_TYPE}, new String[]{"groupValue", "aggregateValue"});
        } else {
            td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateValue"});
        }
        
        
        Iterator it = hm.entrySet().iterator();
        while (it.hasNext()) {
            HashMap.Entry entry = (Map.Entry)it.next();
            Field k = (Field) entry.getKey();
            Integer v = (Integer) entry.getValue();
            if (this.what == Op.AVG) {
                Integer hv = (Integer) hmHelp.get(k);
                v = v / hv;
            }
            Tuple t = new Tuple(td);
            if (this.gbfieldtype != null) {
                t.setField(0, k);
                t.setField(1, new IntField(v));
            } else {
                t.setField(0, new IntField(v));
            }
            res.add(t);
        }
        // 2. iterator
        return new AggregateIterator(res, td);
    }

}

class AggregateIterator implements OpIterator {
    private static final long serialVersionUID = 1L;

    TupleDesc td;
    Vector<Tuple> v;
    Iterator<Tuple> it;

    public AggregateIterator(Vector<Tuple> v, TupleDesc td) {
        this.v = v;
        this.td = td;
    }

    /**
   * Opens the iterator. This must be called before any of the other methods.
   * @throws DbException when there are problems opening/accessing the database.
   */
    public void open() throws DbException, TransactionAbortedException {
        this.it = v.iterator();
    }

    /** Returns true if the iterator has more tuples.
    * @return true f the iterator has more tuples.
    * @throws IllegalStateException If the iterator has not been opened
    */
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (it == null) throw new IllegalStateException("not open");
        return it.hasNext();
    }

    /**
    * Returns the next tuple from the operator (typically implementing by reading
    * from a child operator or an access method).
    *
    * @return the next tuple in the iteration.
    * @throws NoSuchElementException if there are no more tuples.
    * @throws IllegalStateException If the iterator has not been opened
    */
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (it == null) throw new IllegalStateException("not open");
        if (!it.hasNext()) throw new NoSuchElementException("no such element");
        return it.next();
    }

    /**
    * Resets the iterator to the start.
    * @throws DbException when rewind is unsupported.
    * @throws IllegalStateException If the iterator has not been opened
    */
    public void rewind() throws DbException, TransactionAbortedException {
        this.close();
        this.open();
    }

    /**
    * Returns the TupleDesc associated with this OpIterator.
    * @return the TupleDesc associated with this OpIterator.
    */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    /**
    * Closes the iterator. When the iterator is closed, calling next(),
    * hasNext(), or rewind() should fail by throwing IllegalStateException.
    */
    public void close() {
        it = null;
    }
}
