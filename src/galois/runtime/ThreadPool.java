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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;

import util.CPUFunctions;

/**
 * Simple thread pool.
 */
class ThreadPool {
  private final List<Worker> workers;
  private final int numThreads;
  private boolean shutdown;

  /**
   * Create a thread pool with the given number of threads.
   * 
   * @param numThreads
   *          the number of threads in the thread pool
   */
  public ThreadPool(int numThreads) {
    this.numThreads = numThreads;
    workers = new ArrayList<Worker>(numThreads);

    for (int i = 0; i < numThreads; i++) {
      workers.add(startThread(i));
    }
  }

  private Worker startThread(int id) {
    Worker w = new Worker(id);
    Thread t = new Thread(w);
    t.setDaemon(true);
    t.start();
    return w;
  }

  /**
   * Shutdown and release the threads in this thread pool.
   */
  public synchronized void shutdown() {
    shutdown = true;
    for (Worker w : workers) {
      w.start.release();
    }
  }

  public boolean suspend() throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  /**
   * Calls the given function with all the threads in the thread pool.
   * 
   * @param callables
   *          function to call
   * @throws InterruptedException
   *           if a thread was interrupted waiting for shutdown
   * @throws ExecutionException
   *           if an error was encountered while execution the function
   */
  public void callAll(List<? extends Callable<?>> callables) throws InterruptedException, ExecutionException {
    Semaphore end = new Semaphore(0);
    for (int i = 0; i < numThreads; i++) {
      workers.get(i).start(callables.get(i), end);
    }

    end.acquire(numThreads);

    for (Worker w : workers) {
      if (w.error != null)
        throw new ExecutionException(w.error);
    }
  }

  private class Worker implements Runnable {
    private final int id;
    private final Semaphore start;
    private Callable<?> callable;
    private Throwable error;
    private Semaphore end;

    public Worker(int id) {
      this.id = id;
      start = new Semaphore(0);
    }

    private void __stackSamplerRecordMe() throws Exception {
      callable.call();
    }

    public void start(Callable<?> callable, Semaphore end) {
      this.callable = callable;
      this.end = end;
      start.release();
    }

    @Override
    public void run() {
//      int[] map = new int[] { 0, 4, 1, 5, 2, 6, 3, 7 };
//      int[] map = new int[] { 0, 1, 2, 3, 4, 5, 6, 7 };
      CPUFunctions.setThreadAffinity(id);
      
      while (!shutdown) {
        try {
          start.acquire();
          try {
            if (!shutdown) {
              __stackSamplerRecordMe();
            }
          } finally {
            end.release();
          }
        } catch (InterruptedException e) {
          error = e;
        } catch (Throwable e) {
          error = e;
        }
      }
    }
  }
}
