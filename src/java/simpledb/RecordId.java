package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    private final PageId pageId;
    private final int tupleNo;
    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleNo
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleNo) {
        // some code goes here
        this.pageId = pid;
        this.tupleNo = tupleNo;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
        // some code goes here
        return this.tupleNo;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
        return this.pageId;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        if(o == null) {
            return false;
        }
        if(!(o instanceof RecordId)) {
            return false;
        }
        if(!((RecordId) o).pageId.equals(this.pageId) || ((RecordId) o).tupleNo != this.tupleNo) {
            return false;
        }
        return true;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        return this.pageId.hashCode() + this.tupleNo;
    }

}
