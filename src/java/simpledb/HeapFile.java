package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File f;
    private TupleDesc td;
    private String tbName;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
        tbName = Database.getCatalog().addTable(this);
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.f;
    }

    public String getTbName() {
        return this.tbName;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return this.f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        if (pid.getPageNumber() >= numPages()) throw new IllegalArgumentException("no page");
        try {
            RandomAccessFile raf = new RandomAccessFile(this.f, "r");
            raf.seek(pid.getPageNumber() * BufferPool.getPageSize());
            byte data[] = new byte[BufferPool.getPageSize()];
            raf.read(data);
            raf.close();
            return new HeapPage((HeapPageId)pid, data);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        RandomAccessFile raf = new RandomAccessFile(this.f, "rw");
        raf.seek(page.getId().getPageNumber() * BufferPool.getPageSize());
        raf.write(page.getPageData());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        int byteLen = (int)this.f.length();
        return byteLen / BufferPool.getPageSize();
    }


    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> al = insertTupleWithoutDisk(tid, t);
        for (int i=0;i<al.size();i++) {
            writePage(al.get(i));
        }
        return al;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTupleWithoutDisk(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        int num = numPages();
        ArrayList<Page> al = new ArrayList<>();
        for (int i=0;i<num;i++) {
            HeapPage p = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), null);
            // HeapPage p = (HeapPage)readPage(new HeapPageId(getId(), i));
            boolean added = true;
            try{
                p.insertTuple(t);
            } catch (DbException e) {
                added = false;
            }
            if (added) {
                al.add(p);
                break;
            }
        }
        if (al.size() != 0) return al;

        // new page in file
        FileOutputStream fos = new FileOutputStream(f, true);
        byte[] data = new byte[BufferPool.getPageSize()]; // fill 0
        Arrays.fill(data, (byte)0);
        fos.write(data, 0, data.length);
        fos.flush();
        fos.close();
        // try to insert
        HeapPage p = (HeapPage)readPage(new HeapPageId(getId(), numPages()-1));
        boolean added = true;
        try{
            p.insertTuple(t);
        } catch (DbException e) {
            added = false;
        }
        if (added) {
            al.add(p);
        }
        if (al.size() == 0) throw new DbException("can't be added");
        return al;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) 
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> al = deleteTupleWithoutDisk(tid, t);
        for (int i=0;i<al.size();i++) {
            writePage(al.get(i));
        }
        return al;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTupleWithoutDisk(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        int num = numPages();
        ArrayList<Page> al = new ArrayList<>();
        for (int i=0;i<num;i++) {
            HeapPage p = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), null);
            // HeapPage p = (HeapPage)readPage(new HeapPageId(getId(), i));
            boolean deleted = true;
            try{
                p.deleteTuple(t);
            } catch (DbException e) {
                deleted = false;
            }
            if (deleted) {
                al.add(p);
            }
        }
        if (al.size() == 0) throw new DbException("can't delete");
        return al;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

}

/**
 * Helper class that implements the Java Iterator for tuples on a HeapFile
 */
class HeapFileIterator extends AbstractDbFileIterator {
	TransactionId tid;
    HeapFile f;
    
    HeapPageId pid;
    int pgNo;
    HeapPage page;
    Iterator<Tuple> it;
    

	/**
	 * Constructor for this iterator
	 * @param f - the HeapFile containing the tuples
	 * @param tid - the transaction id
	 */
	public HeapFileIterator(HeapFile f, TransactionId tid) {
		this.f = f;
        this.tid = tid;
	}

	/**
	 * Open this iterator
	 */
	public void open() throws DbException, TransactionAbortedException {
        int tbId = Database.getCatalog().getTableId(f.getTbName());
        pgNo = 0;
        pid = new HeapPageId(tbId, pgNo);
        page = (HeapPage)Database.getBufferPool().getPage(tid, pid, null);
        it = page.iterator();
	}

	/**
	 * Read the next tuple either from the current page if it has more tuples or
	 * from the next page by following the right sibling pointer.
	 * 
	 * @return the next tuple, or null if none exists
	 */
	@Override
	protected Tuple readNext() throws TransactionAbortedException, DbException {
        if (pid == null) return null;;
        if (it.hasNext()) {
            return it.next();
        }
        // next
        int tbId = Database.getCatalog().getTableId(f.getTbName());
        pgNo++;

        if (pgNo >= f.numPages()) return null;
        pid = new HeapPageId(tbId, pgNo);
        // page = (HeapPage)f.readPage((PageId)pid);
        page = (HeapPage)Database.getBufferPool().getPage(tid, pid, null);
        if (page == null) return null;
        it = page.iterator();
        if (it.hasNext()) {
            return it.next();
        }
        return null;
	}

	/**
	 * rewind this iterator back to the beginning of the tuples
	 */
	public void rewind() throws DbException, TransactionAbortedException {
		close();
		open();
	}

	/**
	 * close the iterator
	 */
	public void close() {
        super.close();
		pid = null;
        pgNo = 0;
        page = null;
        it = null;
    }
}

