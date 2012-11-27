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
import galois.runtime.IdlenessStatistics.Idleable;
import galois.runtime.wl.UnorderedWorklist;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import util.SystemProperties;
import util.fn.Lambda0Void;
import util.fn.Lambda2Void;

/**
 * An unordered Galois executor.
 * 
 * @param <T>
 *          type of elements being iterated over
 */
class UnorderedExecutor<T> implements Executor {
  private static final boolean useControl = SystemProperties.getBooleanProperty("usecontrol", true);

  private boolean finish;
  private final Condition moreWork;
  private final ReentrantLock moreWorkLock;
  private final AtomicInteger numDone;
  private final Deque<Lambda0Void> suspendThunks;
  private boolean yield;

  private final int numThreads;
  private ProcessGroup<MyProcess> processes;

  public UnorderedExecutor() {
    numThreads = GaloisRuntime.getRuntime().getMaxThreads();
    numDone = new AtomicInteger();
    moreWorkLock = new ReentrantLock();
    moreWork = moreWorkLock.newCondition();
    suspendThunks = new ArrayDeque<Lambda0Void>();
  }

  private synchronized void addSuspendThunk(Lambda0Void callback) {
    suspendThunks.add(callback);
  }

  @Override
  public void arbitrate(Iteration current, Iteration conflicter) throws IterationAbortException {
    IterationAbortException.throwException();
  }

  public final IterationStatistics call(final Lambda2Void<T, ForeachContext<T>> body,
      final UnorderedWorklist<T> worklist) throws ExecutionException {
    processes = new ProcessGroup<MyProcess>(numThreads) {
      @Override
      protected MyProcess newInstance(int id) {
        return new MyProcess(id, body, worklist);
      }
    };

    // Start interrupter
    Interrupter interrupter = new Interrupter(new Lambda0Void() {
      @Override
      public void call() {
        if (processes != null) {
          for (MyProcess p : processes) {
            p.wakeupTick();
          }
        }
      }
    });

    interrupter.start();
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

      return processes.finish();
    } finally {
      interrupter.stop();
      processes = null;
    }
  }

  @Override
  public boolean isSerial() {
    return false;
  }

  private void makeAllDone() {
    // Can't use set() because there is a rare possibility
    // that it would happen between an increment decrement
    // pair in isDone and prevent some threads from leaving
    int n;
    do {
      n = numDone.get();
    } while (!numDone.compareAndSet(n, numThreads));
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

  private void reset() {
    yield = false;
    finish = false;
    suspendThunks.clear();
    numDone.set(0);
  }

  private boolean someDone() {
    return numDone.get() > 0;
  }

  private void wakeupAll() {
    moreWorkLock.lock();
    try {
      moreWork.signalAll();
    } finally {
      moreWorkLock.unlock();
    }

    for (MyProcess p : processes) {
      p.wakeup();
    }
  }

  private void wakeupOne() {
    moreWorkLock.lock();
    try {
      moreWork.signal();
    } finally {
      moreWorkLock.unlock();
    }
  }

  private class MyProcess extends ProcessGroup.Process implements ForeachContext<T> {
    private final UnorderedWorklist<T> worklist;
    private final Lambda2Void<T, ForeachContext<T>> body;
    private int consecAborts;
    private Iteration currentIteration;
    private long lastAbort;

    private final Condition wakeup;
    private final ReentrantLock wakeupLock;

    private int wakeupRound;

    public MyProcess(int id, Lambda2Void<T, ForeachContext<T>> body, UnorderedWorklist<T> worklist) {
      super(id);
      this.body = body;
      this.worklist = worklist;
      wakeupLock = new ReentrantLock();
      wakeup = wakeupLock.newCondition();
    }

    @Override
    public final void add(final T t) {
      add(t, MethodFlag.ALL);
    }

    @Override
    public void add(final T t, byte flags) {
      final ForeachContext<T> ctx = this;
      if (GaloisRuntime.needMethodFlag(flags, MethodFlag.SAVE_UNDO)) {
        currentIteration.addCommitAction(new Lambda0Void() {
          @Override
          public void call() {
            worklist.add(t, ctx);
            if (useControl) {
              if (someDone()) {
                wakeupOne();
              }
            }
          }
        });
      } else {
        worklist.add(t, ctx);
        if (useControl) {
          if (someDone()) {
            wakeupOne();
          }
        }
      }
    }

    @Override
    protected void run() throws Exception {
      Iteration prevIteration = Iteration.getCurrentIteration();
      Iteration.setCurrentIteration(currentIteration);
      try {
        T item = null;
        L1: do {

          while (true) {
            setupCurrentIteration();
            if (item == null && (item = nextItem()) == null) {
              if (yield) {
                break L1;
              } else {
                break;
              }
            }

            try {
              body.call(item, this);
              doCommit(item);
            } catch (IterationAbortException _) {
              readd(item);
              doAbort();
            } catch (WorkNotUsefulException _) {
              doCommit(item);
            } catch (Throwable e) {
              // Gracefully terminate processes
              if (currentIteration != null) {
                incrementAborted();
                currentIteration.performAbort();
              }
              throw new ExecutionException(e);
            }

            if (yield)
              break L1;

            item = null;
          }
        } while ((item = isDone()) != null);

      } finally {
        makeAllDone();
        wakeupAll();
        currentIteration = null;
        Iteration.setCurrentIteration(prevIteration);
      }
    }

    private void doAbort() {
      currentIteration.performAbort();
      incrementAborted();
      if (useControl) {
        final int logFactor = 4;
        final int mask = (1 << logFactor) - 1;
        if (lastAbort == getCommitted()) {
          // Haven't committed anything since last abort
          consecAborts++;
          if (consecAborts > 1 && (consecAborts & mask) == 0) {
            sleep(consecAborts >> logFactor);
          }
        } else {
          consecAborts = 0;
        }
        lastAbort = getCommitted();
      }
    }

    private void doCommit(T item) {
      try {
        currentIteration.performCommit(true);
        incrementCommitted();
      } catch (IterationAbortException _) {
        // Happens when an iteration has thrown
        // WorkNotUsefulException or WorkNotProgressiveException,
        // and tries to commit before it goes to RTC (i.e. completes), another
        // thread signals it to abort itself
        readd(item);
        doAbort();
      }
    }

    private void doFinish() {
      finish = true;
      yield = true;
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

    private T isDone() throws InterruptedException {
      if (!useControl)
        return null;

      startWaiting();
      moreWorkLock.lock();
      try {
        int done = numDone.incrementAndGet();

        if (done > numThreads) {
          // Error by another thread
          return null;
        }

        T item = null;
        while (true) {
          done = numDone.get();
          if (done > numThreads) {
            // Error by another thread
            return null;
          } else if (done < numThreads) {
            if ((item = worklist.poll(this)) != null) {
              break;
            }
          } else {
            // Last man: safe to check global termination property
            item = worklist.polls();
            break;
          }

          moreWork.await();
        }

        if (item == null) {
          assert done == numThreads;
          wakeupAll();
          return null;
        } else {
          numDone.decrementAndGet();
          return item;
        }
      } finally {
        moreWorkLock.unlock();
        stopWaiting();
      }
    }

    private T nextItem() {
      T item;
      try {
        item = worklist.poll(this);
      } catch (IterationAbortException e) {
        throw new Error("Worklist method threw unexpected exception");
      }

      if (item == null) {
        // Release any locks acquired before iteration begins by index or
        // comparison methods
        currentIteration.performCommit(true);
      }
      return item;
    }

    /**
     * Re-add item to worklist in case of abort.
     * 
     * @param item
     */
    private void readd(T item) {
      while (true) {
        try {
          worklist.addAborted(item, this);
          break;
        } catch (IterationAbortException e) {
          // Commonly this exception is never thrown, but
          // client code may provide comparators/indexers
          // that may abort, in which case spin until we
          // can put the item back
        }
      }
    }

    private final void setupCurrentIteration() {
      beginIteration();

      if (currentIteration == null) {
        currentIteration = new Iteration(getThreadId());
        Iteration.setCurrentIteration(currentIteration);
      }
    }

    /**
     * Called by current thread to indicate that it wants to sleep for the given
     * number of milliseconds.
     * 
     * @param millis
     */
    private void sleep(int millis) {
      startWaiting();
      wakeupLock.lock();
      wakeupRound = millis;
      try {
        while (wakeupRound > 0) {
          wakeup.await();
        }
      } catch (InterruptedException e) {
        throw new Error(e);
      } finally {
        wakeupLock.unlock();
        stopWaiting();
      }
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

    /**
     * Unconditionally wake up this process (if possible).
     */
    private void wakeup() {
      wakeupLock.lock();
      try {
        wakeupRound = 0;
        wakeup.signal();
      } finally {
        wakeupLock.unlock();
      }
    }

    /**
     * Called by interrupter thread to advance sleeping processes.
     */
    private void wakeupTick() {
      wakeupLock.lock();
      try {
        if (wakeupRound > 0) {
          if (--wakeupRound <= 0) {
            wakeup.signal();
          }
        }
      } finally {
        wakeupLock.unlock();
      }
    }
  }
}
