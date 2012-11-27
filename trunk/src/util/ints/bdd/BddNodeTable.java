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

 File: BddNodeTable.java
 */

package util.ints.bdd;

import util.concurrent.NotThreadSafe;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public class BddNodeTable {

  /*
   * The basic strategy is to subdivide the table among Segments,
   * each of which itself is a concurrently readable hash table.
   */


  /* ---------------- Constants -------------- */

  /**
   * The default initial capacity for this table,
   * used when not otherwise specified in a constructor.
   */
  static final int DEFAULT_INITIAL_CAPACITY = 2 << 12;

  /**
   * The default load factor for this table, used when not
   * otherwise specified in a constructor.
   */
  static final float DEFAULT_LOAD_FACTOR = 0.75f;

  /**
   * The default concurrency level for this table, used when not
   * otherwise specified in a constructor.
   */
  static final int DEFAULT_CONCURRENCY_LEVEL = 512;

  /**
   * The maximum capacity, used if a higher value is implicitly
   * specified by either of the constructors with arguments.  MUST
   * be a power of two <= 1<<30 to ensure that entries are indexable
   * using ints.
   */
  static final int MAXIMUM_CAPACITY = 2 << 29;

  /**
   * The maximum number of segments to allow; used to bound
   * constructor arguments.
   */
  static final int MAX_SEGMENTS = 2 << 10; // slightly conservative

  /* ---------------- Fields -------------- */

  /**
   * Mask value for indexing into segments. The upper bits of a
   * key's hash code are used to choose the segment.
   */
  final int segmentMask;

  /**
   * Shift value for indexing within segments.
   */
  final int segmentShift;

  /**
   * The segments, each of which is a specialized hash table
   */
  final Segment[] segments;


  /**
   * Returns the segment that should be used for key with given hash
   *
   * @param hash the hash code for the key
   * @return the segment
   */
  final Segment segmentFor(int hash) {
    return segments[(hash >>> segmentShift) & segmentMask];
  }

  private int hashOf(Object key) {
    return HashUtil.hash(key.hashCode());
  }

  private static final class Segment {

    /**
     * The table is rehashed when resizeCountDown == 0
     */
    private int resizeCountDown;

    private static final AtomicIntegerFieldUpdater<Segment> flagUpdater
        = AtomicIntegerFieldUpdater.newUpdater(Segment.class, "flag");

    private volatile int flag;

    /**
     * The per-segment table.
     */
    transient volatile BddNode[] table;


    Segment(int initialCapacity) {
      BddNode[] newTable = new BddNode[initialCapacity];
      resizeCountDown = (int) (newTable.length * DEFAULT_LOAD_FACTOR);
      table = newTable;
      flag = 0;
    }

    @NotThreadSafe
    private boolean put(BddNode key, int hash) {
      BddNode[] tab = table;
      int index = hash & (tab.length - 1);
      BddNode first = tab[index];
      BddNode e = first;
      while (e != null && (e.hash != hash || !key.equals(e))) {
        e = e.next;
      }
      if (e != null) {
        return false;
      } else {
        key.next = first;
        tab[index] = key;
        if (--resizeCountDown == 0) {
          rehash();
        }
      }
      return true;
    }

    private BddNode createAndPutBddNodeIfAbsentReturnKey(int hash, byte var, final BddNode low, final BddNode high) {
      // busy wait behaves better than locking. However, if the number of resize operations is relatively
      // high we are probably better off by using locking, since resizes can take some time to complete.
      while (!flagUpdater.compareAndSet(this, 0, 1)) ;
      try {
        BddNode[] tab = table;
        int index = hash & (tab.length - 1);
        BddNode first = tab[index];
        BddNode e = first;
        while (e != null) {
          if (e.hash == hash && var == e.level && high == e.high && low == e.low) {
            return e;
          }
          e = e.next;
        }
        BddNode key = new BddNode(var, low, high, hash, first);
        tab[index] = key;
        if (--resizeCountDown == 0) {
          rehash();
        }
        return key;
      } finally {
        flagUpdater.set(this, 0);
      }
    }

    void rehash() {
      //System.err.println("Segment resize.");
      BddNode[] oldTable = table;
      int oldCapacity = oldTable.length;
      if (oldCapacity >= MAXIMUM_CAPACITY) {
        return;
      }
      BddNode[] newTable = new BddNode[oldCapacity << 1];
      resizeCountDown = (int) (newTable.length * DEFAULT_LOAD_FACTOR);
      int sizeMask = newTable.length - 1;
      for (int i = 0; i < oldCapacity; i++) {
        BddNode e = oldTable[i];
        while (e != null) {
          // save curr.next, since we are about to modify it
          BddNode nextCurr = e.next;
          int k = e.hash & sizeMask;
          e.next = newTable[k];
          newTable[k] = e;
          e = nextCurr;
        }
      }
      table = newTable;
    }

    @NotThreadSafe
    void clear() {
      BddNode[] tab = table;
      for (int i = 0; i < tab.length; i++) {
        tab[i] = null;
      }
      resizeCountDown = (int) (tab.length * DEFAULT_LOAD_FACTOR);
    }
  }

  /* ---------------- Public operations -------------- */

  /**
   * Creates a new, empty map with the specified initial
   * capacity, reference types, load factor and concurrency level.
   * <p/>
   * Behavioral changing options such as {@link Option#IDENTITY_COMPARISONS}
   * can also be specified.
   *
   * @param initialCapacity  the initial capacity. The implementation
   *                         performs internal sizing to accommodate this many elements.
   * @param loadFactor       the load factor threshold, used to control resizing.
   *                         Resizing may be performed when the average number of elements per
   *                         bin exceeds this threshold.
   * @param concurrencyLevel the estimated number of concurrently
   *                         updating threads. The implementation performs internal sizing
   *                         to try to accommodate this many threads.
   * @param keyType          the reference type to use for keys
   * @param options          the behavioral options
   * @throws IllegalArgumentException if the initial capacity is
   *                                  negative or the load factor or concurrencyLevel are
   *                                  nonpositive.
   */
  public BddNodeTable(int initialCapacity, float loadFactor, int concurrencyLevel) {
    if (!(loadFactor > 0) || initialCapacity < 0 || concurrencyLevel <= 0) {
      throw new IllegalArgumentException();
    }

    if (concurrencyLevel > MAX_SEGMENTS) {
      concurrencyLevel = MAX_SEGMENTS;
    }

    // Find power-of-two sizes best matching arguments
    int sshift = 0;
    int ssize = 1;
    while (ssize < concurrencyLevel) {
      ++sshift;
      ssize <<= 1;
    }
    segmentShift = 32 - sshift;
    segmentMask = ssize - 1;
    this.segments = new Segment[ssize];

    if (initialCapacity > MAXIMUM_CAPACITY) {
      initialCapacity = MAXIMUM_CAPACITY;
    }
    int c = initialCapacity / ssize;
    if (c * ssize < initialCapacity) {
      ++c;
    }
    int cap = 1;
    while (cap < c) {
      cap <<= 1;
    }
    for (int i = 0; i < this.segments.length; ++i) {
      this.segments[i] = new Segment(cap);
    }
  }

  public BddNodeTable() {
    this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, DEFAULT_CONCURRENCY_LEVEL);
  }

  /**
   * @return the previously inserted key if present,
   *         or return the newly inserted key.
   * @throws NullPointerException if the specified key or value is null
   */
  public BddNode putBddNodeIfAbsentReturnKey(byte var, final BddNode low, final BddNode high) {
    int hash = BddNode.computeHash(var, low, high);
    Segment segment = segmentFor(hash);
    return segment.createAndPutBddNodeIfAbsentReturnKey(hash, var, low, high);
  }

  public void putIfAbsent(BddNode key) {
    int hash = hashOf(key);
    segmentFor(hash).put(key, hash);
  }

  /**
   * Removes all of the mappings from this map.
   */
  public void clear() {
    for (int i = 0; i < segments.length; ++i) {
      segments[i].clear();
    }
  }
}
