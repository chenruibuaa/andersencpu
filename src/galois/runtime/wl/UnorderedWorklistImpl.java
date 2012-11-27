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
import galois.runtime.GaloisRuntime;

/**
 * A worklist in two parts: a thread-local worklist and a global worklist.
 * The thread-local worklist is polled from and added to first. Only when the local 
 * worklist is empty is the global worklist examined for {@link #poll(ThreadContext)}.
 * {@link #add(Object, ThreadContext)} always goes to the local worklist.
 * 
 *
 * @param <T>  the type of elements of the worklist
 */
class UnorderedWorklistImpl<T> implements UnorderedWorklist<T> {
  private final Worklist<T> work;
  private final Worklist<T>[] workLocal;
  private final Worklist<T> aborted;
  private final Worklist<T>[] abortedLocal;
  
  /**
   * Creates a worklist from a local and global worklist.
   * 
   * @param workMaker  maker for global worklist
   * @param workLocalMaker  maker for local worklist
   */
  @SuppressWarnings("unchecked")
  public UnorderedWorklistImpl(Lambda0<Worklist<T>> workMaker, Lambda0<Worklist<T>> workLocalMaker, Lambda0<Worklist<T>> abortedMaker, Lambda0<Worklist<T>> abortedLocalMaker) {
    int numThreads = GaloisRuntime.getRuntime().getMaxThreads();
    
    if (workMaker != null)
      this.work = workMaker.call();
    else
      this.work = null;
    
    if (workLocalMaker != null) {
      workLocal = new Worklist[numThreads];
      for (int i = 0; i < numThreads; i++) {
        workLocal[i] = workLocalMaker.call();
      }
    } else {
      workLocal = null;
    }
    
    if (abortedMaker != null) 
      aborted = abortedMaker.call();
     else 
      aborted = null;
    
   if (abortedLocalMaker != null) {
      abortedLocal = new Worklist[numThreads];
      for (int i = 0; i < numThreads; i++) {
        abortedLocal[i] = abortedLocalMaker.call();
      }
    } else {
      abortedLocal = null;
    }
  }

  @Override
  public UnorderedWorklist<T> newInstance() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void add(T item, ThreadContext ctx) {
    if (workLocal != null)
      workLocal[ctx.getThreadId()].add(item, ctx);
    else
      work.add(item, ctx);
  }

  @Override
  public void addAborted(T item, ThreadContext ctx) {
    if (abortedLocal != null)
      abortedLocal[ctx.getThreadId()].add(item, ctx);
    else if (aborted != null)
      aborted.add(item, ctx);
    else if (work != null)
      work.add(item, ctx);
    else
      workLocal[ctx.getThreadId()].add(item, ctx);
  }
  
  @Override
  public T polls() {
    T item;
    if (work != null && (item = work.polls()) != null) {
      return item;
    }
    if (workLocal != null) {
      for (int i = 0; i < workLocal.length; i++) {
        if ((item = workLocal[i].polls()) != null)
          return item;
      }
    }
    if (aborted != null && (item = aborted.polls()) != null) {
      return item;
    }
    if (abortedLocal != null) {
      for (int i = 0; i < abortedLocal.length; i++) {
        if ((item = abortedLocal[i].polls()) != null)
          return item;
      }
    }
    return null;
  }
  
  @Override
  public T poll(ThreadContext ctx) {
    T retval = null;
    int tid = -1;

    if (workLocal != null) {
      tid = tid == -1 ? ctx.getThreadId() : tid;
      retval = workLocal[tid].poll(ctx);
    }

    if (retval == null && work != null)
      retval = work.poll(ctx);
    
    if (retval == null && abortedLocal != null) {
      tid = tid == -1 ? ctx.getThreadId() : tid;
      retval = abortedLocal[tid].poll(ctx);
    }
    
    if (retval == null && aborted != null) {
      retval = aborted.poll(ctx);
    }
        
    return retval;
  }
  
  @Override
  public int size() {
    throw new UnsupportedOperationException();
  }
}
