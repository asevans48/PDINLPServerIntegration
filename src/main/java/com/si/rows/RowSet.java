package com.si.rows;
/**
 * Allows for additions at a row.
 *
 * @author Andrew Evans
 */

import java.util.*;

/**
 * Row iterator
 */
public class RowSet implements Set<Object[]>, Iterable<Object[]> {

    private List<Object[]> set;

    private class RowSetIterator implements Iterator<Object[]> {

        /**
         * Whether there is another row in the iterator
         * @return  whether or not there is a row
         */
        @Override
        public boolean hasNext() {
            if(set.size() > 0){
                return true;
            }
            return false;
        }

        /**
         * Get the next row in the iterator
         * @return  The next row
         */
        @Override
        public Object[] next() {
            if(set.size() > 0){
                Object[] row = set.get(0);
                set.remove(0);
                return row;
            }
            return null;
        }

        /**
         * Remove the first item in the set
         */
        @Override
        public void remove() {
            if(set.size() > 0){
                set.remove(0);
            }
        }
    }

    /**
     * Constructor with complete row set
     * @param rows  Incoming rows
     */
    public RowSet(Object[][] rows){
        set = Arrays.asList(rows);
    }

    /**
     * Constructor for empty row set iterator
     */
    public RowSet(){
        this.set = new ArrayList<Object[]>();
    }


    /**
     * Get the size of the set
     * @return  The size of the set
     */
    @Override
    public int size() {
        return set.size();
    }

    /**
     * Whether the set is empty
     * @return  whether the set is empty
     */
    @Override
    public boolean isEmpty() {
        return set.isEmpty();
    }

    /**
     * Whether the set contains an object
     * @param o The object
     * @return  Whether the set contains the object
     */
    @Override
    public boolean contains(Object o) {
        return set.contains(o);
    }

    /**
     * Obtain an Iterator for the rowset
     * @return
     */
    @Override
    public Iterator<Object[]> iterator() {
        return new RowSetIterator();
    }

    /**
     * Convert row set to an array
     * @return  The row array
     */
    @Override
    public Object[] toArray() {
        return this.set.toArray();
    }

    /**
     *
     * @param a
     * @param <T>
     * @return
     */
    @Override
    public <T> T[] toArray(T[] a) {
        T[] arr = a;
        if(a.length != this.set.size()){
            arr = (T[]) new Object[this.set.size()];
        }
        for(int i = 0; i < a.length; i++){
            arr[i] = (T) this.set.get(i);
        }
        return arr;
    }

    /**
     * Add rows to the set.
     *
     * @param objects   The
     * @return  Whether the objects were added
     */
    @Override
    public boolean add(Object[] objects) {
        return this.set.add(objects);
    }

    /**
     * Remove from the set (more of a way to conform)
     * @param o The object row to remove (will remove by reference)
     * @return  Whether the row was removed
     */
    @Override
    public boolean remove(Object o) {
        return this.set.remove(o);
    }

    /**
     * Wether the set contains the objects
     * @param c The collection
     * @return  Whether the set contains the collection
     */
    @Override
    public boolean containsAll(Collection<?> c) {
        return this.set.containsAll(c);
    }

    /**
     * Add all of the objects to the set
     *
     * @param c The objects to add
     * @return  Whether the objects were added
     */
    @Override
    public boolean addAll(Collection<? extends Object[]> c) {
        return this.set.addAll(c);
    }

    /**
     * Retain only these objects
     * @param c The objects to retain
     * @return  Whether the objects were retained
     */
    @Override
    public boolean retainAll(Collection<?> c) {
        return this.set.retainAll(c);
    }

    /**
     * Remove all objects
     * @param c The objects to remove
     * @return  Wehther hte objects were removed
     */
    @Override
    public boolean removeAll(Collection<?> c) {
        return this.removeAll(c);
    }

    /**
     * Clear the set
     */
    @Override
    public void clear() {
        this.set.clear();
    }
}