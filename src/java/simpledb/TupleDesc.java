package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return this.fieldName + "(" + this.fieldType + ")";
        }
    }

    private List<TDItem> items;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return this.items.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        this.items = new ArrayList<>(typeAr.length);
        for(int i = 0; i < typeAr.length; i++) {
            String fieldName = fieldAr[i] == null ? "null" : fieldAr[i];
//            String fieldName = fieldAr[i] == null ? "" : fieldAr[i];
            TDItem item = new TDItem(typeAr[i], fieldName);
            this.items.add(item);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this.items = new ArrayList<>(typeAr.length);
        for(Type type : typeAr) {
//            this.items.add(new TDItem(type, null));
            this.items.add(new TDItem(type, ""));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.items.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if(i < 0 || i >= this.items.size()) {
            throw new NoSuchElementException("invalid field index " + i);
        }
        return this.items.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if(i < 0 || i >= this.items.size()) {
            throw new NoSuchElementException("invalid field index " + i);
        }
        return this.items.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        // assume that if name is null, then throw exception directly
        if(name == null) {
            throw new NoSuchElementException("name cannot be null");
        }
        for (int i = 0; i < this.items.size(); i++) {
            if(name.equals(this.items.get(i).fieldName)) {
                return i;
            }
        }
        throw new NoSuchElementException("cannot find field name: " + name);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int tupleSize = 4;
        return tupleSize * this.numFields();
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        Type[] mergeTypes = new Type[td1.numFields() + td2.numFields()];
        String[] mergeFieldNames = new String[td1.numFields() + td2.numFields()];
        int index = 0;
        for (int i = 0; i < td1.numFields(); i++) {
            mergeTypes[index] = td1.getFieldType(i);
            mergeFieldNames[index] = td1.getFieldName(i);
            index++;
        }
        for (int i = 0; i < td2.numFields(); i++) {
            mergeTypes[index] = td2.getFieldType(i);
            mergeFieldNames[index] = td2.getFieldName(i);
            index++;
        }
        return new TupleDesc(mergeTypes, mergeFieldNames);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        if(o == null) {
            return false;
        }
        if(!(o instanceof TupleDesc)) {
            return false;
        }
        if(((TupleDesc) o).numFields() != this.numFields()) {
            return false;
        }
        for(int i = 0; i < this.numFields(); i++) {
            if(this.getFieldName(i) != ((TupleDesc) o).getFieldName(i)) {
                return false;
            }
            if(this.getFieldType(i) != ((TupleDesc) o).getFieldType(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns a hash code value for the object. This method is
     * supported for the benefit of hash tables such as those provided by
     * {@link HashMap}.
     * <p>
     * The general contract of {@code hashCode} is:
     * <ul>
     * <li>Whenever it is invoked on the same object more than once during
     *     an execution of a Java application, the {@code hashCode} method
     *     must consistently return the same integer, provided no information
     *     used in {@code equals} comparisons on the object is modified.
     *     This integer need not remain consistent from one execution of an
     *     application to another execution of the same application.
     * <li>If two objects are equal according to the {@code equals(Object)}
     *     method, then calling the {@code hashCode} method on each of
     *     the two objects must produce the same integer result.
     * <li>It is <em>not</em> required that if two objects are unequal
     *     according to the {@link Object#equals(Object)}
     *     method, then calling the {@code hashCode} method on each of the
     *     two objects must produce distinct integer results.  However, the
     *     programmer should be aware that producing distinct integer results
     *     for unequal objects may improve the performance of hash tables.
     * </ul>
     * <p>
     * As much as is reasonably practical, the hashCode method defined by
     * class {@code Object} does return distinct integers for distinct
     * objects. (This is typically implemented by converting the internal
     * address of the object into an integer, but this implementation
     * technique is not required by the
     * Java&trade; programming language.)
     *
     * @return a hash code value for this object.
     * @see Object#equals(Object)
     * @see System#identityHashCode
     */

    public int hashCode() {
    // If you want to use TupleDesc as keys for HashMap, implement this so
    // that equal objects have equals hashCode() results
//        throw new UnsupportedOperationException("unimplemented");
        return super.hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < this.items.size(); i++) {
            String fieldTypeStr = this.items.get(i).fieldType.toString();
            String fieldNameStr = this.items.get(i).fieldName == null ? "": this.items.get(i).fieldName;
            sb.append(fieldTypeStr)
                    .append("(")
                    .append(fieldNameStr)
                    .append("), ");
        }
        sb.deleteCharAt(sb.length() - 2);   // remove the last ", "
        return sb.toString();
    }
}
