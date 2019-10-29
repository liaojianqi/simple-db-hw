package simpledb;


import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;
import java.util.NoSuchElementException;
import java.lang.IllegalStateException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    int gbfield;
    Type gbfieldtype;
    int afield;
    Op what;

    HashMap<Field, Integer> hm; // group value -> aggregate value

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
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("only support COUNT");
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        this.hm = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field f = tup.getField(this.gbfield);
        if (f == null) {
            System.out.println("=========never occur!=====");
            return ;
        }
        Integer v = hm.get(f);
        if (v == null) {
            hm.put(f, 1);
            return;
        }
        hm.put(f, v+1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
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
