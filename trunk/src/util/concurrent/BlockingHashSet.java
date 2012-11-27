/*
 Galois, a framework to exploit amorphous data-parallelism in irregular
 programs.

 Copyright (C) 2010, The University of Texas at Austin. All rights reserved.
 UNIVERSITY EXPRESSLY DISCLAIMS ANY AND ALL WARRANTIES CONCERNING THIS SOFTWARE
 AND DOCUMENTATION, INCLUDING ANY WARRANTIES OF MERCHANTABILITY, FITNESS FOR ANY
 PARTICULAR PURPOSE, NON-INFRINGEMENT AND WARRANTIES OF PERFORMANCE, AND ANY
 WARRANTY THAT MIGHT OTHERWISE ARISE FROM COURSE OF DEALING OR USAGE OF TRADE.
 NO WARRANTY IS EITHER EXPRESS OR IMPLIED WITH RESPECT TO THE USE OF THE
 SOFTWARE OR DOCUMENTATION. Under no circumstances shall University be liable
 for incidental, special, indirect, direct or consequential damages or loss of
 profits, interruption of business, or related expenses which may arise from use
 of Software or Documentation, including but not limited to those resulting from
 defects in Software and/or Documentation, or loss or inaccuracy of data of any
 kind.

 File: BlockingHashSet.java
 */


package util.concurrent;

import java.io.IOException;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class implements the <tt>Set</tt> interface, backed by a ConcurrentHashMap instance.
 *
 * @author Matt Tucker
 */
public class BlockingHashSet<E> extends AbstractSet<E> implements Set<E>, Cloneable {

  private transient ConcurrentHashMap<E, Object> map;

  // Dummy value to associate with an Object in the backing Map
  private static final Object PRESENT = new Object();

  /**
   * Constructs a new, empty set; the backing <tt>ConcurrentHashMap</tt> instance has
   * default initial capacity (16) and load factor (0.75).
   */
  public BlockingHashSet() {
    map = new ConcurrentHashMap<E, Object>();
  }

  /**
   * Constructs a new set containing the elements in the specified
   * collection.  The <tt>ConcurrentHashMap</tt> is created with default load factor
   * (0.75) and an initial capacity sufficient to contain the elements in
   * the specified collection.
   *
   * @param c the collection whose elements are to be placed into this set.
   * @throws NullPointerException if the specified collection is null.
   */
  public BlockingHashSet(Collection<? extends E> c) {
    map = new ConcurrentHashMap<E, Object>(Math.max((int) (c.size() / .75f) + 1, 16));
    addAll(c);
  }

  public BlockingHashSet(int initialCapacity, float loadFactor, int concurrencyLevel) {
    map = new ConcurrentHashMap<E, Object>(initialCapacity, loadFactor, concurrencyLevel);
  }

  /**
   * Constructs a new, empty set; the backing <tt>ConcurrentHashMap</tt> instance has
   * the specified initial capacity and the specified load factor.
   *
   * @param initialCapacity the initial capacity of the hash map.
   * @param loadFactor      the load factor of the hash map.
   * @throws IllegalArgumentException if the initial capacity is less
   *                                  than zero, or if the load factor is nonpositive.
   */
  public BlockingHashSet(int initialCapacity, float loadFactor) {
    map = new ConcurrentHashMap<E, Object>(initialCapacity, loadFactor, 16);
  }

  /**
   * Constructs a new, empty set; the backing <tt>HashMap</tt> instance has
   * the specified initial capacity and default load factor, which is
   * <tt>0.75</tt>.
   *
   * @param initialCapacity the initial capacity of the hash table.
   * @throws IllegalArgumentException if the initial capacity is less
   *                                  than zero.
   */
  public BlockingHashSet(int initialCapacity) {
    map = new ConcurrentHashMap<E, Object>(initialCapacity);
  }

  public Iterator<E> iterator() {
    return map.keySet().iterator();
  }

  public int size() {
    return map.size();
  }

  public boolean isEmpty() {
    return map.isEmpty();
  }

  public boolean contains(Object o) {
    return map.containsKey(o);
  }

  public boolean add(E o) {
    return map.put(o, PRESENT) == null;
  }

  public boolean remove(Object o) {
    return map.remove(o) == PRESENT;
  }

  public void clear() {
    map.clear();
  }

  @SuppressWarnings("unchecked")
  public Object clone() {
    try {
      BlockingHashSet<E> newSet = (BlockingHashSet<E>) super.clone();
      newSet.map.putAll(map);
      return newSet;
    } catch (CloneNotSupportedException e) {
      throw new InternalError();
    }
  }

  private void writeObject(java.io.ObjectOutputStream s) throws java.io.IOException {
    s.defaultWriteObject();

    // Write out size
    s.writeInt(map.size());

    // Write out all elements in the proper order.
    for (Iterator<E> i = map.keySet().iterator(); i.hasNext();) {
      s.writeObject(i.next());
    }
  }

  /**
   * Reconstitute the <tt>HashSet</tt> instance from a stream (that is,
   * deserialize it).
   */
  @SuppressWarnings("unchecked")
  private void readObject(java.io.ObjectInputStream s) throws IOException, ClassNotFoundException {
    s.defaultReadObject();

    map = new ConcurrentHashMap<E, Object>();

    // Read in size
    int size = s.readInt();

    // Read in all elements in the proper order.
    for (int i = 0; i < size; i++) {
      E e = (E) s.readObject();
      map.put(e, PRESENT);
    }
  }
}
