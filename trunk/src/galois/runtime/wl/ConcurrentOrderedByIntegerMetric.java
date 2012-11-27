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
import galois.runtime.GaloisRuntime;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import util.fn.Lambda;
import util.fn.Lambda0;

@MatchingConcurrentVersion(ConcurrentOrderedByIntegerMetric.class)
@MatchingLeafVersion(ConcurrentOrderedByIntegerMetricLeaf.class)
class ConcurrentOrderedByIntegerMetric<T> implements Worklist<T> {
  private static final int CACHE_MULTIPLE = 16;
  private static final int APPROX_RANGE = 2048;

  private final Lambda<T, Integer> indexer;
  private Worklist<T>[] bucket;
  private final boolean descending;
  private final int[] cursor;
  private AtomicInteger size;
  private final boolean approxMonotonic;

  public ConcurrentOrderedByIntegerMetric(int range, Lambda<T, Integer> indexer, Lambda0<Worklist<T>> maker,
      boolean needSize) {
    this(range, indexer, false, false, maker, needSize);
  }

  @SuppressWarnings("unchecked")
  public ConcurrentOrderedByIntegerMetric(int range, Lambda<T, Integer> indexer, boolean descending,
      boolean approxMonotonic, Lambda0<Worklist<T>> maker, boolean needSize) {
    this(range, indexer, descending, approxMonotonic, (Worklist<T>[]) null, needSize);

    range = approxMonotonic ? APPROX_RANGE : range + 1;
    bucket = new Worklist[range];
    for (int i = 0; i < bucket.length; i++) {
      bucket[i] = maker.call();
    }
  }

  private ConcurrentOrderedByIntegerMetric(int range, Lambda<T, Integer> indexer, boolean descending,
      boolean approxMonotonic, Worklist<T>[] bucket, boolean needSize) {
    this.indexer = indexer;
    this.descending = descending;
    this.bucket = bucket;
    this.approxMonotonic = approxMonotonic;

    int numThreads = GaloisRuntime.getRuntime().getMaxThreads();
    cursor = new int[numThreads * CACHE_MULTIPLE]; // Make cache-friendly

    if (descending) {
      int top = approxMonotonic ? APPROX_RANGE - 1 : range;
      Arrays.fill(cursor, top);
    }

    if (needSize)
      size = new AtomicInteger();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Worklist<T> newInstance() {
    Worklist<T>[] b = new Worklist[bucket.length];
    for (int i = 0; i < bucket.length; i++) {
      b[i] = bucket[i].newInstance();
    }
    return new ConcurrentOrderedByIntegerMetric<T>(bucket.length - 1, indexer, descending, approxMonotonic, b,
        size != null);
  }

  private int getIndex(int tid) {
    return tid * CACHE_MULTIPLE;
  }

  private int getBucket(int index) {
    if (approxMonotonic) {
      return index % APPROX_RANGE;
    } else {
      return index;
    }
  }

  @Override
  public void add(T item, ThreadContext ctx) {
    int tid = ctx.getThreadId();
    int index = getBucket(indexer.call(item));

    if (size != null)
      size.incrementAndGet();

    bucket[index].add(item, ctx);

    if (!approxMonotonic) {
      if (descending) {
        if (index > cursor[getIndex(tid)])
          cursor[getIndex(tid)] = index;
      } else {
        if (index < cursor[getIndex(tid)])
          cursor[getIndex(tid)] = index;
      }
    }
  }

  private T pollMonotonic(ThreadContext ctx) {
    int tid = ctx.getThreadId();
    int index = cursor[getIndex(tid)];
    T retval = null;

    for (int i = 0; i < APPROX_RANGE; i++) {
      retval = bucket[index].poll(ctx);
      if (retval != null) {
        cursor[getIndex(tid)] = index;
        break;
      }

      index += descending ? -1 : 1;
      if (index < 0) {
        index = bucket.length - 1;
      } else if (index > bucket.length - 1) {
        index = 0;
      }
    }
    return retval;
  }

  private T pollNormal(ThreadContext ctx) {
    int tid = ctx.getThreadId();
    int index = cursor[getIndex(tid)];
    T retval = null;

    while (index < bucket.length && index >= 0) {
      retval = bucket[index].poll(ctx);
      if (retval != null)
        break;
      index += descending ? -1 : 1;
    }

    if (retval != null) {
      if (cursor[getIndex(tid)] != index) {
        cursor[getIndex(tid)] = index;
      }
    } else {
      if (descending)
        cursor[getIndex(tid)] = bucket.length - 1;
      else
        cursor[getIndex(tid)] = 0;
    }
    return retval;
  }

  @Override
  public T poll(ThreadContext ctx) {
    T retval = approxMonotonic ? pollMonotonic(ctx) : pollNormal(ctx);

    if (retval != null && size != null)
      size.decrementAndGet();

    return retval;
  }

  @Override
  public T polls() {
    T item;
    int delta = descending ? -1 : 1;
    int start = descending ? bucket.length : 0;

    for (int cur = start; cur < bucket.length && cur >= 0; cur += delta) {
      if ((item = bucket[cur].polls()) != null) {
        if (size != null)
          size.decrementAndGet();
        for (int idx = 0; idx < cursor.length; idx += CACHE_MULTIPLE)
          cursor[idx] = cur;
        return item;
      }
    }
    return null;
  }

  @Override
  public int size() {
    if (size != null)
      return size.get();
    else
      throw new UnsupportedOperationException();
  }
}
