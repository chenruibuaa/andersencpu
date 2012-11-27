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
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import util.fn.Lambda0;

@NeedsSize
@MatchingLeafVersion(ConcurrentOrderedLeaf.class)
@MatchingConcurrentVersion(ConcurrentOrdered.class)
class ConcurrentOrdered<T> extends AbstractUnorderedWorklist<T> implements OrderedWorklist<T> {
  private final Worklist<T> empty;
  private final ConcurrentSkipListMap<T, Worklist<T>> map;
  private final Comparator<T> comp;
  private final Comparator<T> innerComp;
  private final Lock readRootLock;
  private final Lock writeRootLock;
  private final AtomicInteger sizeLock;
  private AtomicInteger size;

  public ConcurrentOrdered(Comparator<T> comp, Lambda0<Worklist<T>> maker, boolean needSize) {
    this(comp, maker.call(), needSize);
  }

  private ConcurrentOrdered(Comparator<T> comp, Worklist<T> empty, boolean needSize) {
    this.comp = comp;
    this.empty = empty;
    map = new ConcurrentSkipListMap<T, Worklist<T>>(comp);
    ReadWriteLock lock = new ReentrantReadWriteLock();
    readRootLock = lock.readLock();
    writeRootLock = lock.writeLock();
    sizeLock = new AtomicInteger();

    if (empty instanceof OrderedWorklist) {
      innerComp = ((OrderedWorklist<T>) empty).getComparator();
    } else {
      innerComp = null;
    }

    if (needSize)
      size = new AtomicInteger();
  }

  @Override
  public Worklist<T> newInstance() {
    return new ConcurrentOrdered<T>(comp, empty, size != null);
  }

  private void addExclusive(ThreadContext ctx, T item) {
    if (size != null) {
      size.incrementAndGet();
    }
    addIfNotPresent(item).add(item, ctx);
  }

  @Override
  public void add(T item, ThreadContext ctx) {
    readRootLock.lock();
    try {
      if (comp.compare(item, map.firstKey()) >= 0) {
        // Commutes invariant held
        if (size != null) {
          size.incrementAndGet();
        }
        addIfNotPresent(item).add(item, ctx);
        return;
      }
    } catch (NoSuchElementException e) {
      if (size != null) {
        size.incrementAndGet();
      }
      addIfNotPresent(item).add(item, ctx);
      return;
    } finally {
      readRootLock.unlock();
    }
    addExclusive(ctx, item);
  }

  private Worklist<T> addIfNotPresent(T item) {
    Worklist<T> retval = map.get(item);

    if (retval == null) {
      Worklist<T> nextWorklist;
      try {
        nextWorklist = empty.newInstance();
      } catch (Exception e) {
        throw new Error(e);
      }
      Worklist<T> prevWorklist = map.putIfAbsent(item, nextWorklist);
      if (prevWorklist == null) {
        retval = nextWorklist;
      } else {
        retval = prevWorklist;
      }
    }

    return retval;
  }

  @Override
  public T poll(ThreadContext ctx) {
    // loop is only here so we can do a non-local jump from break
    while (true) {
      readRootLock.lock();
      try {
        Map.Entry<T, Worklist<T>> entry = map.firstEntry();
        if (entry == null) {
          return null;
        }

        T retval;
        try {
          int readers;
          if ((readers = sizeLock.incrementAndGet()) <= 0 || entry.getValue().size() <= readers + 1) {
            break;
          }

          retval = entry.getValue().poll(ctx);
        } finally {
          sizeLock.decrementAndGet();
        }

        // if (entry.getValue().isEmpty())
        // map.pollFirstEntry();

        if (size != null && retval != null) {
          size.decrementAndGet();
        }

        return retval;
      } finally {
        readRootLock.unlock();
      }
    }

    return pollExclusive(ctx);
  }

  private T pollExclusive(ThreadContext ctx) {
    writeRootLock.lock();
    try {
      Map.Entry<T, Worklist<T>> entry;

      while ((entry = map.firstEntry()) != null) {
        T item = entry.getValue().polls();
        if (item == null) {
          map.pollFirstEntry();
        } else {
          if (size != null)
            size.decrementAndGet();
          return item;
        }
      }
      return null;
    } finally {
      writeRootLock.unlock();
    }
  }

  @Override
  public T peek() {
    // TODO(ddn): Only works when there is one level of total ordering
    Map.Entry<T, Worklist<T>> entry = map.firstEntry();
    if (entry == null) {
      return null;
    }
    return entry.getKey();
  }

  @Override
  public T polls() {
    return poll(null);
  }

  @Override
  public int size() {
    if (size != null) {
      return size.get();
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public Comparator<T> getComparator() {
    if (innerComp == null)
      return comp;

    return new Comparator<T>() {
      @Override
      public int compare(T arg0, T arg1) {
        int r = comp.compare(arg0, arg1);
        if (r == 0) {
          return innerComp.compare(arg0, arg1);
        }
        return r;
      }
    };
  }
}
