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

import java.util.concurrent.ExecutionException;

import util.fn.Lambda0Void;
import util.fn.LambdaVoid;

class PmapExecutor<T> implements Executor, PmapContext {
  private Object contextObject;

  public PmapExecutor() {
  }

  public IterationStatistics call(final Mappable<T> mappable, final LambdaVoid<T> body) throws ExecutionException {
    ProcessGroup<MyProcess> processes = new ProcessGroup<MyProcess>(GaloisRuntime.getRuntime().getMaxThreads()) {
      @Override
      protected MyProcess newInstance(int id) {
        return new MyProcess(id, mappable, body);
      }
    };
    
    mappable.beforePmap(this);
    try {
      processes.run();
      return processes.finish();
    } finally {
      mappable.afterPmap(this);
      contextObject = null;
    }
  }

  @Override
  public boolean isSerial() {
    // NB: For the purpose of flags, this is serial
    return true;
  }

  @Override
  public int getThreadId() {
    throw new UnsupportedOperationException();
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
    throw new UnsupportedOperationException();
  }

  @Override
  public void onUndo(Iteration it, Lambda0Void action) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void onRelease(Iteration it, ReleaseCallback action) {
    throw new UnsupportedOperationException();
  }
  
  private class MyProcess extends ProcessGroup.Process implements PmapContext {
    private LambdaVoid<T> body;
    private Mappable<T> mappable;
    
    public MyProcess(int id, Mappable<T> mappable, LambdaVoid<T> body) {
      super(id);
      this.mappable = mappable;
      this.body = body;
    }

    protected void run() {
      LambdaVoid<T> wrapped = new LambdaVoid<T>() {
        @Override
        public void call(T arg0) {
          beginIteration();
          body.call(arg0);
          incrementCommitted();
        }
      };
      
      mappable.pmap(wrapped, this);
    }

    @Override
    public void setContextObject(Object obj) {
      PmapExecutor.this.setContextObject(obj);
    }

    @Override
    public Object getContextObject() {
      return PmapExecutor.this.getContextObject();
    }
  }
}
