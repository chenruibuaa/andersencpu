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

import util.fn.Lambda0;
import galois.runtime.ThreadContext;

/**
 * Order elements in first-in-first-order order, i.e., queue order. This order may only
 * contain up to <i>N</i> elements.
 * 
 *
 * @param <T>  the type of elements of the worklist
 * @see FIFO
 */
@OnlyLeaf
@MatchingConcurrentVersion(ConcurrentBoundedFIFO.class)
@MatchingLeafVersion(BoundedFIFO.class)
public class BoundedFIFO<T> implements Worklist<T> {
  public static final int DEFAULT_MAX_ELEMENTS = 32;

  private final Object[] buffer;
  private int widx;
  private int ridx;
  private int size;

  /**
   * Creates a bounded FIFO order with the default maximum number of elements ({@value #DEFAULT_MAX_ELEMENTS})
   */
  public BoundedFIFO(Lambda0<Worklist<T>> maker, boolean needSize) {
    this(DEFAULT_MAX_ELEMENTS, maker, needSize);
  }

  /**
   * Creates a bounded FIFO order with the given maximum number of elements
   * @param maxElements  the maximum number of elements
   */
  public BoundedFIFO(int maxElements, Lambda0<Worklist<T>> maker, boolean needSize) {
    buffer = new Object[maxElements];
    widx = 0;
    ridx = 0;
  }

  @Override
  public Worklist<T> newInstance() {
    return new BoundedFIFO<T>(buffer.length, null, false);
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public void add(T item, ThreadContext ctx) {
    buffer[widx] = item;
    if (++widx == buffer.length)
      widx = 0;
    size++;
  }

  @SuppressWarnings("unchecked")
  @Override
  public T poll(ThreadContext ctx) {
    if (size == 0)
      return null;

    Object v = buffer[ridx];
    buffer[ridx] = null;

    if (++ridx == buffer.length)
      ridx = 0;

    size--;

    return (T) v;
  }
  
  @Override
  public T polls() {
    return poll(null);
  }

}
