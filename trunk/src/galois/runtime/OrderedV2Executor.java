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

import galois.objects.MethodFlag;
import galois.runtime.wl.Priority;
import galois.runtime.wl.UnorderedWorklist;
import galois.runtime.wl.Worklists;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.logging.Logger;

import util.concurrent.ConcurrentSPCQueue;
import util.fn.Lambda;
import util.fn.Lambda0Void;
import util.fn.Lambda2Void;

class OrderedV2Executor<T> implements Executor {
  private static final Logger logger = Logger.getLogger("galois.runtime.Executor");
  private static final int ITERATION_POOL_SIZE = 16;
  private static final int WORK_ITEM_QUEUE_SIZE = 16;
  private static final int IMMEDIATE_QUEUE_SIZE = 16;

  private boolean finish;
  private boolean yield;
  private boolean isDone;

  private final AtomicInteger numDone;
  private final int numThreads;
  private final int numWorkers;

  private final Deque<Lambda0Void> suspendThunks;
  private ProcessGroup<MyProcess> processes;

  public OrderedV2Executor() {
    numThreads = GaloisRuntime.getRuntime().getMaxThreads();
    numWorkers = numThreads - 1;
    numDone = new AtomicInteger();
    suspendThunks = new ArrayDeque<Lambda0Void>();
  }

  private synchronized void addSuspendThunk(Lambda0Void callback) {
    suspendThunks.add(callback);
  }

  public final IterationStatistics call(final Lambda2Void<T, ForeachContext<T>> body, Iterable<T> initial,
      Priority.Rule order, Priority.Rule priority) throws ExecutionException {
    Lambda<WorkItem<T>, T> unwrapper = new Lambda<WorkItem<T>, T>() {
      @Override
      public T call(WorkItem<T> arg0) {
        return arg0.item;
      }
    };

    final UnorderedWorklist<WorkItem<T>> worklist = Priority.makeUnordered(priority == null ? order : priority,
        numThreads < 2, unwrapper);
    final UnorderedWorklist<WorkItem<T>> serialWorklist = Priority.makeUnordered(order, true, unwrapper);

    List<WorkItem<T>> wrapped = new ArrayList<WorkItem<T>>();
    for (T item : initial) {
      WorkItem<T> w = new WorkItem<T>(item);
      serialWorklist.add(w, null);
      wrapped.add(w);
    }
    Worklists.initialWorkDistribution(worklist, wrapped, numWorkers);

    processes = new ProcessGroup<MyProcess>(numThreads) {
      @Override
      protected MyProcess newInstance(int id) {
        return new MyProcess(id, body, serialWorklist, worklist);
      }
    };

    try {
      while (true) {
        reset();

        processes.run();

        if (!suspendThunks.isEmpty()) {
          GaloisRuntime.getRuntime().replaceWithRootContextAndCall(new Lambda0Void() {
            public void call() {
              for (Lambda0Void thunk : suspendThunks) {
                thunk.call();
              }
            }
          });
        }

        if (finish || !yield) {
          break;
        }
      }

      for (MyProcess p : processes) {
        System.out.printf("POOLS: %d it: %d wq: %d iq: %d\n", p.getThreadId(), p.iterationPool.newInstanceCount,
            p.workItemQueue.newInstanceCount, p.immediateQueue.newInstanceCount);
      }

      return processes.finish();
    } finally {
      processes = null;
    }

  }

  @Override
  public boolean isSerial() {
    return false;
  }

  @Override
  public void onCommit(Iteration it, Lambda0Void action) {
    it.addCommitAction(action);
  }

  @Override
  public void onRelease(Iteration it, ReleaseCallback action) {
    it.addReleaseAction(action);
  }

  @Override
  public void onUndo(Iteration it, Lambda0Void action) {
    it.addUndoAction(action);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void arbitrate(Iteration current, Iteration conflicter) throws IterationAbortException {
    OrderedV2Iteration me = (OrderedV2Iteration) current;
    OrderedV2Iteration other = (OrderedV2Iteration) conflicter;
    if (other != null && ((WorkItem<T>) me.getCurrentWorkItem()).isLeast()) {
      // TODO(ddn): Mark waiting time?
      while (other.isAborting())
        ;

      if (other.isReadyToCommit()) {
        WorkItem<T> w = (WorkItem<T>) other.getCurrentWorkItem();
        MyProcess p = processes.get(me.getId());
        // XXX: this will be buggy when there is more than one least work item,
        // as unclaim will unconditionally release and thus claim won't be able
        // to guarantee mutual exclusion
        p.abort(w);
        logger.fine(String.format("[xxx] %d aborted other\n", me.getId()));
      } else {
        logger.fine(String.format("[xxx] %d other aborted?\n", me.getId()));
      }
      return;
    }

    IterationAbortException.throwException();
  }

  private void reset() {
    yield = false;
    finish = false;
    isDone = false;
    suspendThunks.clear();
    numDone.set(0);
  }

  /**
   * Object pool with concurrent single-producer, single-consumer recycling.
   * 
   * @param <U>
   */
  private static class Pool<U> {
    private final ConcurrentSPCQueue<U> pool;
    private ConcurrentLinkedQueue<U> queue;
    private int newInstanceCount;

    /**
     * 
     * @param maxSize
     * @param newInstances
     *          if true, create new instances if necessary
     */
    public Pool(int maxSize, boolean newInstances) {
      pool = new ConcurrentSPCQueue<U>(maxSize);
      if (!newInstances) {
        queue = new ConcurrentLinkedQueue<U>();
      }
    }

    protected U newInstance() {
      throw new UnsupportedOperationException();
    }

    public U poll() {
      U retval = pool.poll();
      if (retval == null) {
        if (queue == null) {
          newInstanceCount++;
          return newInstance();
        } else {
          return queue.poll();
        }
      }
      return retval;
    }

    public void add(U item) {
      if (!pool.add(item) && queue != null) {
        newInstanceCount++;
        queue.add(item);
      }
    }
  }

  private static class WorkItem<U> {
    private static AtomicReferenceFieldUpdater<WorkItem, OrderedV2Iteration> updater = AtomicReferenceFieldUpdater
        .newUpdater(WorkItem.class, OrderedV2Iteration.class, "it");
    private U item;
    private volatile OrderedV2Iteration it;
    private int distributeCount;
    private boolean least;

    public WorkItem(U item) {
      this.item = item;
    }

    public OrderedV2Iteration getIteration() {
      return it;
    }

    public void setLeast(boolean least) {
      this.least = least;
    }

    public boolean isLeast() {
      return least;
    }

    public boolean claim(OrderedV2Iteration it) {
      return (this.it == null) ? updater.compareAndSet(this, null, it) : false;
    }

    public void unclaim() {
      // XXX: why are multiple threads calling this?
      it.performAbort();
      it = null;
    }

    public boolean canDistribute(int maxDistributes) {
      return distributeCount < maxDistributes;
    }

    public WorkItem<U> distribute() {
      distributeCount++;
      return this;
    }
  }

  private class MyProcess extends ProcessGroup.Process implements ForeachContext<T> {
    private OrderedV2Iteration currentIteration;

    private final Pool<OrderedV2Iteration> iterationPool;
    private final Pool<WorkItem<T>> workItemQueue;
    private final Pool<WorkItem<T>> immediateQueue;
    private final Deque<WorkItem<T>> selfImmediateQueue;
    private final Lambda2Void<T, ForeachContext<T>> body;
    private final UnorderedWorklist<WorkItem<T>> serialWorklist;
    private final UnorderedWorklist<WorkItem<T>> worklist;
    
    public MyProcess(int id, Lambda2Void<T, ForeachContext<T>> body, UnorderedWorklist<WorkItem<T>> serialWorklist, UnorderedWorklist<WorkItem<T>> worklist) {
      super(id);
      this.body = body;
      this.serialWorklist = serialWorklist;
      this.worklist = worklist;
      
      iterationPool = new Pool<OrderedV2Iteration>(ITERATION_POOL_SIZE, true) {
        @Override
        public OrderedV2Iteration newInstance() {
          return new OrderedV2Iteration(getThreadId());
        }
      };
      workItemQueue = new Pool<WorkItem<T>>(WORK_ITEM_QUEUE_SIZE, false);
      immediateQueue = new Pool<WorkItem<T>>(IMMEDIATE_QUEUE_SIZE, false);
      selfImmediateQueue = new ArrayDeque<WorkItem<T>>();
    }

    @Override
    protected void run() throws ExecutionException, InterruptedException {
      Iteration prevIteration = Iteration.getCurrentIteration();
      Iteration.setCurrentIteration(currentIteration);
      try {
        if (getThreadId() == numWorkers) {
          doCallCommitter();
        } else {
          doCall();
        }
      } finally {
        Iteration.setCurrentIteration(prevIteration);
      }
    }

    private void doCallCommitter() {
      WorkItem<T> workItem;
      int victim = 0;

      try {
        L1: while ((workItem = serialWorklist.poll(null)) != null) {
          // TODO(ddn): Mark wait time?
          workItem.setLeast(true);
          while (true) {
            if (yield || isDone) {
              break L1;
            }
            OrderedV2Iteration it = workItem.getIteration();

            if (it == null) {
              if (workItem.canDistribute(numWorkers)) {
                MyProcess p = processes.get(victim++ % numWorkers);
                p.immediateQueue.add(workItem.distribute());
                logger.fine("[serial] distributed");
              } else {
                logger.fine("[serial] waiting it == null");
              }
            } else if (it.isReadyToCommit()) {
              break;
            } else {
              logger.fine("[serial] waiting not RTC");
            }
          }
          serialCommit(workItem);
          logger.fine("[serial] committed " + workItem);
          if (finish) {
            // XXX: abort outstanding iterations
            throw new Error();
          }
        }
        logger.fine("[serial] WHY?");
      } finally {
        isDone = true;
      }
    }

    private final void serialCommit(WorkItem<T> workItem) {
      OrderedV2Iteration it = workItem.getIteration();
      MyProcess p = processes.get(it.getId());

      List<T> items = (List<T>) it.getItems();
      for (int i = 0; i < items.size(); i++) {
        T item = items.get(i);
        WorkItem<T> w = new WorkItem<T>(item);
        serialWorklist.add(w, null);
        p.workItemQueue.add(w);
      }

      commitIteration(it, p.iterationPool);
      incrementCommitted();
    }

    private void commitIteration(OrderedV2Iteration it, Pool<OrderedV2Iteration> pool) {
      it.performCommit(true);
      pool.add(it);
    }

    private WorkItem<T> nextItem(WorkItem<T> isDoneWorkItem) {
      beginIteration();

      // (1) Drain work item queue
      WorkItem<T> item;
      while ((item = workItemQueue.poll()) != null) {
        worklist.add(item, this);
      }

      currentIteration = iterationPool.poll();
      Iteration.setCurrentIteration(currentIteration);

      // (2) Get next item
      try {
        while (true) {
          if (isDoneWorkItem != null) {
            item = isDoneWorkItem;
            isDoneWorkItem = null;
          } else if ((item = selfImmediateQueue.poll()) != null) {
            ;
          } else if ((item = immediateQueue.poll()) != null) {
            // System.out.println("[xxx] get something");
          } else {
            item = worklist.poll(this);
          }

          if (item == null) {
            // Release any locks acquired before iteration begins by index or
            // comparison methods
            currentIteration.setReadyToCommit();
            commitIteration(currentIteration, iterationPool);
          } else if (item.claim(currentIteration)) {
            currentIteration.setCurrentWorkItem(item);
          } else {
            continue;
          }
          return item;
        }
      } catch (IterationAbortException e) {
        throw new Error("Worklist method threw unexpected exception");
      }
    }

    private void doCall() throws ExecutionException, InterruptedException  {
      try {
        WorkItem<T> workItem = null;
        L1: do {

          while (true) {
            if ((workItem = nextItem(workItem)) == null) {
              logger.fine("[xxx] nextItem == null");
              if (yield) {
                break L1;
              } else {
                break;
              }
            }

            try {
              body.call(workItem.item, this);
              currentIteration.setReadyToCommit();
            } catch (IterationAbortException _) {
              abort(workItem);
            } catch (Throwable e) {
              abort(workItem);
              throw new ExecutionException(e);
            }

            if (yield)
              break L1;

            workItem = null;
          }
        } while ((workItem = isDone()) != null);
        logger.fine("[xxx] leaving");
      } finally {
        currentIteration = null;
        isDone = true;
      }
    }

    private void doFinish() {
      finish = true;
      yield = true;
    }

    @Override
    public final void add(final T t) {
      add(t, MethodFlag.ALL);
    }

    @Override
    public void add(final T t, byte flags) {
      currentIteration.addItem(t);
    }

    @Override
    public void finish() {
      currentIteration.addCommitAction(new Lambda0Void() {
        @Override
        public void call() {
          doFinish();
        }
      });
    }

    private WorkItem<T> isDone() throws InterruptedException {
      startWaiting();
      try {
        int done = numDone.incrementAndGet();

        if (done > numWorkers) {
          // Error by another thread
          return null;
        }

        WorkItem<T> workItem = null;
        while (true) {
          done = numDone.get();
          if (done > numWorkers) {
            // Error by another thread
            return null;
          } else if ((workItem = selfImmediateQueue.poll()) != null) {
            break;
          } else if ((workItem = immediateQueue.poll()) != null) {
            break;
          } else if (done < numWorkers && (workItem = worklist.poll(this)) != null) {
            break;
          } else if (done == numWorkers && (workItem = worklist.polls()) != null) {
            // Last man: safe to check polls()
            break;
          } else if (isDone) {
            break;
          }

          // moreWork.await();
        }

        if (workItem == null) {
          isDone = true;
          return null;
        } else {
          numDone.decrementAndGet();
          return workItem;
        }
      } finally {
        stopWaiting();
      }
    }

    /**
     * Re-add item to worklist in case of abort.
     * 
     * @param item
     */
    private void abort(WorkItem<T> item) {
      while (true) {
        try {
          if (item.isLeast())
            selfImmediateQueue.add(item);
          else
            worklist.addAborted(item, this);
          break;
        } catch (IterationAbortException e) {
          // Commonly this exception is never thrown, but
          // client code may provide comparators/indexers
          // that may abort, in which case spin until we
          // can put the item back
        }
      }
      item.unclaim();
      incrementAborted();
    }

    @Override
    public void suspendWith(final Lambda0Void call) {
      currentIteration.addCommitAction(new Lambda0Void() {
        @Override
        public void call() {
          addSuspendThunk(call);
          yield = true;
        }
      });
    }
  }
}
