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


 */

package galois.runtime.wl;

import galois.runtime.ThreadContext;
import util.fn.Lambda;
import util.fn.Lambda0;

/**
 * Order elements according to function mapping elements to integers. Elements
 * are ordered in ascending or descending integer order. Elements with equal
 * integers are unordered with respect to each other.
 * 
 * 
 * @param <T>
 *          the type of elements of the worklist
 */
@MatchingConcurrentVersion(ConcurrentOrderedByIntegerMetric.class)
@MatchingLeafVersion(OrderedByIntegerMetricLeaf.class)
@FunctionParameter(FunctionParameterType.LAMBDA_T_INT_1)
public class OrderedByIntegerMetric<T> implements Worklist<T> {
  private final Lambda<T, Integer> indexer;
  private Worklist<T>[] bucket;
  private int size;
  private int cursor;
  private final boolean descending;

  /**
   * Creates an ascending order
   * 
   * @param range
   *          domain of indexing function is [0, range - 1]
   * @param indexer
   *          function mapping elements to integers [0, range - 1]
   */
  public OrderedByIntegerMetric(int range, Lambda<T, Integer> indexer, Lambda0<Worklist<T>> maker, boolean needSize) {
    this(range, indexer, false, false, maker, needSize);
  }

  /**
   * Creates an ascending or descending order
   * 
   * @param range
   *          domain of indexing function is [0, range - 1]
   * @param descending
   *          true if descending order, otherwise ascending
   * @param indexer
   *          function mapping elements to integers [0, range - 1]
   */
  @SuppressWarnings("unchecked")
  public OrderedByIntegerMetric(int range, Lambda<T, Integer> indexer, boolean descending, boolean approxMonotonic,
      Lambda0<Worklist<T>> maker, boolean needSize) {
    this(range, indexer, descending, approxMonotonic, (Worklist<T>[]) null, needSize);
    bucket = new Worklist[range + 1];
    for (int i = 0; i < bucket.length; i++) {
      bucket[i] = maker.call();
    }
  }

  private OrderedByIntegerMetric(int range, Lambda<T, Integer> indexer, boolean descending, boolean approxMonotonic,
      Worklist<T>[] bucket, boolean needSize) {
    this.indexer = indexer;
    this.descending = descending;
    this.bucket = bucket;

    size = 0;
    if (descending)
      cursor = range;
    else
      cursor = 0;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Worklist<T> newInstance() {
    Worklist<T>[] b = new Worklist[bucket.length];
    for (int i = 0; i < bucket.length; i++) {
      b[i] = bucket[i].newInstance();
    }
    return new OrderedByIntegerMetric<T>(bucket.length - 1, indexer, descending, false, b, false);
  }

  @Override
  public void add(T item, ThreadContext ctx) {
    int index = indexer.call(item);

    size++;
    bucket[index].add(item, ctx);

    if (descending) {
      if (cursor <= index)
        cursor = index;
    } else {
      if (cursor >= index)
        cursor = index;
    }
  }

  @Override
  public T poll(ThreadContext ctx) {
    int cur;
    T retval = null;

    while ((cur = cursor) < bucket.length && cur >= 0) {
      retval = bucket[cur].poll(ctx);
      if (retval == null) {
        if (descending)
          cursor--;
        else
          cursor++;
      } else {
        break;
      }
    }

    if (retval != null) {
      size--;
    }

    return retval;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public T polls() {
    return poll(null);
  }
}
