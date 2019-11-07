package simpledb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    int tableid;
    int ioCostPerPage;
    DbFile df;
    TupleDesc td;
    int sz;
    Vector<Object> vs;
    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.

        // some code goes here
        this.ioCostPerPage = ioCostPerPage;
        df = Database.getCatalog().getDatabaseFile(tableid);
        td = df.getTupleDesc();
        sz = td.numFields();
        Vector<Integer> max = new Vector<>(sz);
        Vector<Integer> min = new Vector<>(sz);
        Vector<Boolean> hasSet = new Vector<>(sz);
        for (int i=0;i<sz;i++) {
            max.add(i, -1);
            min.add(i, -1); // value is not care
            hasSet.add(false);
        }
        vs = new Vector<>(sz);

        // 1. get min and max
        DbFileIterator it = df.iterator(new TransactionId());
        try {
            it.open();
            while (it.hasNext()) {
                Tuple t = it.next();
                for (int i=0;i<sz;i++) {
                    Field f = t.getField(i);
                    if (f.getType() == Type.INT_TYPE) {
                        IntField intF = (IntField)f;
                        if (!hasSet.get(i)) {
                            max.set(i, intF.getValue());
                            min.set(i, intF.getValue());
                            hasSet.set(i, true);
                        } else {
                            if (intF.getValue() > max.get(i)) max.set(i, intF.getValue());
                            if (intF.getValue() < min.get(i)) min.set(i, intF.getValue());
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        // 2. add histogram of fields
        for (int i=0;i<sz;i++) {
            if (td.getFieldType(i) == Type.INT_TYPE) {
                IntHistogram ih = new IntHistogram(NUM_HIST_BINS, min.get(i), max.get(i));
                vs.add(ih);
            } else {
                StringHistogram sh = new StringHistogram(NUM_HIST_BINS);
                vs.add(sh);
            }
        }
        // 3. add value of fields
        try {
            it.rewind();
            while (it.hasNext()) {
                Tuple t = it.next();
                for (int i=0;i<sz;i++) {
                    Field f = t.getField(i);
                    if (f.getType() == Type.INT_TYPE) {
                        IntField intF = (IntField)f;
                        IntHistogram ih = (IntHistogram)vs.get(i);
                        ih.addValue(intF.getValue());
                    } else {
                        StringField sf = (StringField)f;
                        StringHistogram sh = (StringHistogram)vs.get(i);
                        sh.addValue(sf.getValue());
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        HashSet<PageId> hs = new HashSet<>();
        DbFileIterator it = df.iterator(new TransactionId());
        try {
            it.open();
            while (it.hasNext()) {
                Tuple t = it.next();
                hs.add(t.getRecordId().getPageId());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return hs.size() * (double)ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        int total = 0;
        DbFileIterator it = df.iterator(new TransactionId());
        try {
            it.open();
            while (it.hasNext()) {
                it.next();
                total++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return (int)(total * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        Object o = vs.get(field);
        if (td.getFieldType(field) == Type.INT_TYPE) {
            IntHistogram ih = (IntHistogram)o;
            // System.out.println(ih.toString());
            IntField intF =  (IntField)constant;
            return ih.estimateSelectivity(op, intF.getValue());
        } else {
            StringHistogram sh = (StringHistogram)o;
            StringField sf = (StringField)constant;
            return sh.estimateSelectivity(op, sf.getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return 0;
    }

}
