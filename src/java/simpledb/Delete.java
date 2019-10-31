package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    TransactionId tid;
    OpIterator child;

    Tuple ret;
    Tuple actRet;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId tid, OpIterator child) {
        // some code goes here
        this.tid = tid;
        this.child = child;

        // delete
        try{
            int cnt = 0;
            child.open();
            while (child.hasNext()) {
                Tuple t = child.next();
                Database.getBufferPool().deleteTuple(tid, t);
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
