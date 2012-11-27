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

package galois.runtime;

import galois.objects.Mappable;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ExecutionException;

import util.fn.Lambda0Void;
import util.fn.LambdaVoid;

class SerialPmapExecutor<T> implements Executor, PmapContext {
  private final Mappable<T> mappable;
  private final Deque<Lambda0Void> suspendThunks;
  private int numCommitted;
  private Object contextObject;
  
  public SerialPmapExecutor(Mappable<T> mappable) {
    this.mappable = mappable;
    suspendThunks = new ArrayDeque<Lambda0Void>();
  }

  public IterationStatistics call(LambdaVoid<T> body) throws ExecutionException {
    numCommitted = 0;
    
    mappable.beforePmap(this);
    try {
      mappable.pmap(body, this);
    } finally {
      mappable.afterPmap(this);
      contextObject = null;
    }
    
    IterationStatistics stats = new IterationStatistics();
    stats.putStats(Thread.currentThread(), numCommitted, 0);
    return stats;
  }

  public int getIterationId() {
    return 0;
  }

  @Override
  public int getThreadId() {
    return 0;
  }

  @Override
  public void setContextObject(Object obj) {
    contextObject = obj;
  }

  @Override
  public Object getContextObject() {
    return contextObject;
  }
  
  @Override
  public void arbitrate(Iteration current, Iteration conflicter) throws IterationAbortException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void onCommit(Iteration it, Lambda0Void action) {
    suspendThunks.addFirst(action);
  }

  @Override
  public void onRelease(Iteration it, ReleaseCallback action) {
  }

  @Override
  public void onUndo(Iteration it, Lambda0Void action) {
  }

  @Override
  public boolean isSerial() {
    return true;
  }
}
