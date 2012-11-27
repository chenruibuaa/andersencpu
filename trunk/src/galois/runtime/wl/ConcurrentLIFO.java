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

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import util.fn.Lambda0;

@OnlyLeaf
@MatchingConcurrentVersion(ConcurrentLIFO.class)
@MatchingLeafVersion(ConcurrentLIFO.class)
class ConcurrentLIFO<T> implements Worklist<T> {
  private final LinkedBlockingDeque<T> queue;
  private AtomicInteger size;

  public ConcurrentLIFO(Lambda0<Worklist<T>> maker, boolean needSize) {
    queue = new LinkedBlockingDeque<T>();
    if (needSize)
      size = new AtomicInteger();
  }

  @Override
  public Worklist<T> newInstance() {
    return new ConcurrentLIFO<T>(null, size != null);
  }

  @Override
  public void add(T item, ThreadContext ctx) {
    if (queue.add(item) && size != null) {
      size.incrementAndGet();
    }
  }

  @Override
  public T polls() {
    return poll(null);
  }

  @Override
  public T poll(ThreadContext ctx) {
    T retval = queue.pollLast();
    if (size != null && retval != null) {
      size.decrementAndGet();
    }
    return retval;
  }

  @Override
  public int size() {
    if (size != null)
      return size.get();
    else
      throw new UnsupportedOperationException();
  }
}
