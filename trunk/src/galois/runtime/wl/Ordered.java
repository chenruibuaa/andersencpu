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
import java.util.TreeMap;

import util.fn.Lambda0;

/**
 * Order elements according to user-defined comparison function. Elements
 * considered equal by comparison function are ordered by next rule in
 * the rule sequence.
 * 
 *
 * @param <T>  the type of elements of the worklist
 */
@NeedsSize
@MatchingLeafVersion(OrderedLeaf.class)
@MatchingConcurrentVersion(ConcurrentOrdered.class)
@FunctionParameter(FunctionParameterType.COMPARATOR_0)
public class Ordered<T> extends AbstractUnorderedWorklist<T> implements OrderedWorklist<T> {
  private final Worklist<T> empty;
  private final TreeMap<T, Worklist<T>> map;
  private final Comparator<T> comp;
  private final Comparator<T> innerComp;
  private int size;

  /**
   * Creates the rule using the given comparator
   * 
   * @param comp      function that orders elements
   */
  public Ordered(Comparator<T> comp, Lambda0<Worklist<T>> maker, boolean needSize) {
    this(comp, maker.call(), needSize);
  }

  private Ordered(Comparator<T> comp, Worklist<T> empty, boolean needSize) {
    this.empty = empty;
    this.comp = comp;

    if (empty instanceof OrderedWorklist) {
      innerComp = ((OrderedWorklist<T>) empty).getComparator();
    } else {
      innerComp = null;
    }

    map = new TreeMap<T, Worklist<T>>(comp);
  }

  @Override
  public Worklist<T> newInstance() {
    return new Ordered<T>(comp, empty, false);
  }

  @Override
  public void add(T item, ThreadContext ctx) {
    size++;
    addIfNotPresent(item).add(item, ctx);
  }

  private Worklist<T> addIfNotPresent(T item) {
    Worklist<T> retval = map.get(item);

    if (retval == null) {
      retval = empty.newInstance();
      map.put(item, retval);
    }

    return retval;
  }

  @Override
  public T poll(ThreadContext ctx) {
    Map.Entry<T, Worklist<T>> entry;
    
    while ((entry = map.firstEntry()) != null) {
      T retval = entry.getValue().poll(ctx);
      if (retval == null) {
        map.pollFirstEntry();
      } else {
        size--;
        return retval;
      }
    }
    return null;
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
    return size;
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
