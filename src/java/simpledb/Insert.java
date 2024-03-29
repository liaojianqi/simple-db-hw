package simpledb;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    TransactionId tid;
    OpIterator child;
    int tableId;

    Tuple ret;
    Tuple actRet;

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
    public Insert(TransactionId tid, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.tid = tid;
        this.child = child;
        this.tableId = tableId;

        // insert
        try{
            int cnt = 0;
            child.open();
            while (child.hasNext()) {
                Tuple t = child.next();
                Database.getBufferPool().insertTuple(tid, tableId, t);
                cnt++;
            }
            actRet = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));
            actRet.setField(0, new IntField(cnt));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return actRet.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        ret = actRet;
    }

    public void close() {
        // some code goes here
        ret = null;
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
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
        Tuple tmp = ret;
        ret = null;
        return tmp;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        child = children[0];
    }
}
