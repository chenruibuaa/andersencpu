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

import galois.runtime.GaloisRuntime;
import galois.runtime.ThreadContext;
import util.fn.Lambda0;

@MatchingLeafVersion(WorkStealing.class)
@MatchingConcurrentVersion(WorkStealing.class)
public class WorkStealing<T> implements Worklist<T> {
  /**
   * Absolute bound for parallelism level. Twice this number must fit into a
   * 16bit field to enable word-packing for some counts.
   */
  private static final int MAX_THREADS = 0x7fff;

  /**
   * Array holding all worker threads in the pool. Array size must be a power of
   * two. Updates and replacements are protected by workerLock, but the array is
   * always kept in a consistent enough state to be randomly accessed without
   * locking by workers performing work-stealing, as well as other
   * traversal-based methods in this class. All readers must tolerate that some
   * array slots may be null.
   */
  volatile WorkStealingQueue<T>[] workers;

  /**
   * Lifecycle control. The low word contains the number of workers that are
   * (probably) executing tasks. This value is atomically incremented before a
   * worker gets a task to run, and decremented when worker has no tasks and
   * cannot find any. Bits 16-18 contain runLevel value. When all are zero, the
   * pool is running. Level transitions are monotonic (running -> shutdown ->
   * terminating -> terminated) so each transition adds a bit. These are bundled
   * together to ensure consistent read for termination checks (i.e., that
   * runLevel is at least SHUTDOWN and active threads is zero).
   */
  private volatile int runState;

  // Note: The order among run level values matters.
  private static final int RUNLEVEL_SHIFT = 16;
  private static final int ACTIVE_COUNT_MASK = (1 << RUNLEVEL_SHIFT) - 1;
  private static final int ONE_ACTIVE = 1; // active update delta

  public WorkStealing(Lambda0<Worklist<T>> maker, boolean needSize) {
    this(false, maker, needSize);
  }

  /**
   * Creates a {@code ForkJoinPool} with the given parallelism and thread
   * factory.
   * 
   * @param parallelism
   *          the parallelism level
   * @param factory
   *          the factory for creating new threads
   * @throws IllegalArgumentException
   *           if parallelism less than or equal to zero, or greater than
   *           implementation limit
   * @throws NullPointerException
   *           if the factory is null
   * @throws SecurityException
   *           if a security manager exists and the caller is not permitted to
   *           modify threads because it does not hold
   *           {@link java.lang.RuntimePermission}{@code ("modifyThread")}
   */
  @SuppressWarnings("unchecked")
  public WorkStealing(boolean locallyFifo, Lambda0<Worklist<T>> maker, boolean needSize) {
    int parallelism = GaloisRuntime.getRuntime().getMaxThreads();
    int arraySize = initialArraySizeFor(parallelism);
    this.workers = new WorkStealingQueue[arraySize];
    for (int i = 0; i < parallelism; i++) {
      workers[i] = new WorkStealingQueue<T>(locallyFifo, this);
    }
  }

  /**
   * Returns initial power of two size for workers array.
   * 
   * @param pc
   *          the initial parallelism level
   */
  private static int initialArraySizeFor(int pc) {
    // See Hackers Delight, sec 3.2. We know MAX_THREADS < (1 >>> 16)
    int size = pc < MAX_THREADS ? pc + 1 : MAX_THREADS;
    size |= size >>> 1;
    size |= size >>> 2;
    size |= size >>> 4;
    size |= size >>> 8;
    return size + 1;
  }

  @Override
  public void add(T item, ThreadContext ctx) {
    workers[ctx.getThreadId()].pushTask(item);
  }

  @Override
  public T poll(ThreadContext ctx) {
    T item = workers[ctx.getThreadId()].poll();
    return item;
  }

  public boolean isEmpty() {
    return (runState & ACTIVE_COUNT_MASK) == 0;
  }
  
  @Override
  public T polls() {
    T item;
    for (int i = 0; i < workers.length; i++) {
      if (workers[i] == null)
        break;
      if ((item = workers[i].poll()) != null)
        return item;
    }
    return null;
  }

  @Override
  public int size() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Worklist<T> newInstance() {
    throw new UnsupportedOperationException();
  }

  /**
   * Callback from workers invoked upon each top-level action (i.e.,
   * stealing a task or taking a submission and running
   * it). Performs one or both of the following:
   *
   * * If the worker cannot find work, updates its active status to
   * inactive and updates activeCount unless there is contention, in
   * which case it may try again (either in this or a subsequent
   * call).  Additionally, awaits the next task event and/or helps
   * wake up other releasable waiters.
   *
   * * If there are too many running threads, suspends this worker
   * (first forcing inactivation if necessary).  If it is not
   * resumed before a keepAlive elapses, the worker may be "trimmed"
   * -- killed while suspended within suspendAsSpare. Otherwise,
   * upon resume it rechecks to make sure that it is still needed.
   *
   * @param w the worker
   * @param worked false if the worker scanned for work but didn't
   * find any (in which case it may block waiting for work).
   */
  final void preStep(WorkStealingQueue<T> w, boolean worked) {
    boolean active = w.active;
    boolean inactivate = !worked & active;
    if (inactivate) {
      int c = runState;
      if (UNSAFE.compareAndSwapInt(this, runStateOffset, c, c - ONE_ACTIVE))
        w.active = false;
    }
  }

  /**
   * Advances eventCount and releases waiters until interference by other
   * releasing threads is detected.
   */
  final void signalWork() {

  }

  /**
   * Tries incrementing active count; fails on contention. Called by workers
   * before executing tasks.
   * 
   * @return true on success
   */
  final boolean tryIncrementActiveCount() {
    int c;
    return UNSAFE.compareAndSwapInt(this, runStateOffset, c = runState, c + ONE_ACTIVE);
  }

  /**
   * Tries decrementing active count; fails on contention. Called when workers
   * cannot find tasks to run.
   */
  final boolean tryDecrementActiveCount() {
    int c;
    return UNSAFE.compareAndSwapInt(this, runStateOffset, c = runState, c - ONE_ACTIVE);
  }

  // Unsafe mechanics

  private static final sun.misc.Unsafe UNSAFE = getUnsafe();
  private static final long runStateOffset = objectFieldOffset("runState", WorkStealing.class);

  private static long objectFieldOffset(String field, Class<?> klazz) {
    try {
      return UNSAFE.objectFieldOffset(klazz.getDeclaredField(field));
    } catch (NoSuchFieldException e) {
      // Convert Exception to corresponding Error
      NoSuchFieldError error = new NoSuchFieldError(field);
      error.initCause(e);
      throw error;
    }
  }

  /**
   * Returns a sun.misc.Unsafe. Suitable for use in a 3rd party package. Replace
   * with a simple call to Unsafe.getUnsafe when integrating into a jdk.
   * 
   * @return a sun.misc.Unsafe
   */
  private static sun.misc.Unsafe getUnsafe() {
    try {
      return sun.misc.Unsafe.getUnsafe();
    } catch (SecurityException se) {
      try {
        return java.security.AccessController
            .doPrivileged(new java.security.PrivilegedExceptionAction<sun.misc.Unsafe>() {
              public sun.misc.Unsafe run() throws Exception {
                java.lang.reflect.Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
                f.setAccessible(true);
                return (sun.misc.Unsafe) f.get(null);
              }
            });
      } catch (java.security.PrivilegedActionException e) {
        throw new RuntimeException("Could not initialize intrinsics", e.getCause());
      }
    }
  }

}
