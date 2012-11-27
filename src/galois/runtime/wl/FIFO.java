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

import java.util.ArrayDeque;

import util.fn.Lambda0;

/**
 * Order elements in first-in-first-out order, i.e., queue order.
 * 
 *
 * @param <T>  the type of elements of the worklist
 * @see LIFO
 * @see ChunkedFIFO
 * @see BoundedFIFO
 */
@OnlyLeaf
@MatchingConcurrentVersion(ConcurrentFIFO.class)
@MatchingLeafVersion(FIFO.class)
public class FIFO<T> implements Worklist<T> {
  private ArrayDeque<T> queue;

  public FIFO(Lambda0<Worklist<T>> maker, boolean needSize) {
    queue = new ArrayDeque<T>();
  }

  @Override
  public Worklist<T> newInstance() {
    return new FIFO<T>(null, false);
  }

  @Override
  public void add(T item, ThreadContext ctx) {
    queue.add(item);
  }

  @Override
  public T polls() {
    return poll(null);
  }

  @Override
  public T poll(ThreadContext ctx) {
    return queue.poll();
  }

  @Override
  public int size() {
    return queue.size();
  }
}
