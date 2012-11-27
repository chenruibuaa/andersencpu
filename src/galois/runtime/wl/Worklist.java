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
import util.concurrent.NotThreadSafe;

/**
 * Worklist used by Galois iterators. Worklists are not intended to be
 * instantiated directly but rather by passing an ordering rule to
 * the iterator.
 * 
 *
 * @param <T>  type of elements contained in the worklist
 * @see galois.runtime.wl.Priority.Rule
 * @see galois.runtime.GaloisRuntime#foreach(Iterable, util.fn.Lambda2Void, galois.runtime.wl.Priority.Rule)
 */
interface Worklist<T> {
  /**
   * Adds an element to this worklist. This method is used for newly generated
   * elements or elements added during Galois execution. Thread-safe.
   * 
   * @param item  the item to add
   * @param ctx   an executor context
   */
  public void add(T item, ThreadContext ctx);

  /**
   * Removes an element from this worklist. Thread-safe.
   * 
   * @param ctx   an executor context
   * @return      an element or <code>null</code> if there are no more elements in this
   *              worklist
   */
  public T poll(ThreadContext ctx);

  /**
   * Removes an element from this worklist. <b>Not</b> thread-safe.
   * @return      an element or <code>null</code> if there are no more elements in this
   *              worklist
   */
  @NotThreadSafe
  public T polls();
  
  /**
   * @return  the number of elements in this worklist, during concurrent execution
   *          this may be a lower bound on the number of elements
   */
  public int size();

  /**
   * @return  a new, empty instance of this worklist
   */
  public Worklist<T> newInstance();
}
