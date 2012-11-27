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

import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;

import util.fn.Lambda0;

@OnlyLeaf
@MatchingLeafVersion(ConcurrentOrderedLeaf.class)
@MatchingConcurrentVersion(ConcurrentOrderedLeaf.class)
class ConcurrentOrderedLeaf<T> extends AbstractUnorderedWorklist<T> implements OrderedWorklist<T> {
  private final PriorityBlockingQueue<T> queue;
  private final Comparator<T> comp;

  public ConcurrentOrderedLeaf(Comparator<T> comp, Lambda0<Worklist<T>> maker, boolean needSize) {
    this.comp = comp;
    queue = new PriorityBlockingQueue<T>(111, comp);
  }

  @Override
  public Worklist<T> newInstance() {
    return new ConcurrentOrderedLeaf<T>(comp, null, false);
  }

  @Override
  public void add(T item, ThreadContext ctx) {
    queue.add(item);
  }

  @Override
  public T poll(ThreadContext ctx) {
    return queue.poll();
  }

  @Override
  public T peek() {
    return queue.peek();
  }

  @Override
  public T polls() {
    return poll(null);
  }

  @Override
  public int size() {
    return queue.size();
  }

  @Override
  public Comparator<T> getComparator() {
    return comp;
  }
}
