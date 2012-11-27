package galois.runtime.wl;

import java.util.Random;
import java.util.concurrent.RejectedExecutionException;

class WorkStealingQueue<T> {
  /**
   * Maximum work-stealing queue array size. Must be less than or equal to 1 <<
   * 28 to ensure lack of index wraparound. (This is less than usual bounds,
   * because we need leftshift by 3 to be in int range).
   */
  private static final int MAXIMUM_QUEUE_CAPACITY = 1 << 28;

  /**
   * Capacity of work-stealing queue array upon initialization. Must be a power
   * of two. Initial size must be at least 4, but is padded to minimize cache
   * effects.
   */
  private static final int INITIAL_QUEUE_CAPACITY = 1 << 13;

  /**
   * Generator for initial random seeds for random victim selection. This is
   * used only to create initial seeds. Random steals use a cheaper xorshift
   * generator per steal attempt. We expect only rare contention on
   * seedGenerator, so just use a plain Random.
   */
  private static final Random seedGenerator = new Random();

  /**
   * The pool this thread works in. Accessed directly by ForkJoinTask.
   */
  private final WorkStealing<T> pool;

  /**
   * The work-stealing queue array. Size must be a power of two. Initialized in
   * onStart, to improve memory locality.
   */
  private T[] queue;

  /**
   * Index (mod queue.length) of next queue slot to push to or pop from. It is
   * written only by owner thread, and accessed by other threads only after
   * reading (volatile) base. Both sp and base are allowed to wrap around on
   * overflow, but (sp - base) still estimates size.
   */
  private int sp;

  /**
   * Index (mod queue.length) of least valid queue slot, which is always the
   * next position to steal from if nonempty.
   */
  private volatile int base;

  /**
   * Activity status. When true, this worker is considered active. Accessed
   * directly by pool. Must be false upon construction.
   */
  boolean active;

  /**
   * Seed for random number generator for choosing steal victims. Uses Marsaglia
   * xorshift. Must be initialized as nonzero.
   */
  private int seed;

  /**
   * Number of steals, transferred and reset in pool callbacks pool when idle
   * Accessed directly by pool.
   */
  int stealCount;

  /**
   * True if use local fifo, not default lifo, for local polling. Shadows value
   * from ForkJoinPool, which resets it if changed pool-wide.
   */
  private final boolean locallyFifo;

  private boolean foundLocal;
  private boolean prevRan;

  /**
   * Creates a WorkStealingQueue operating in the given pool.
   * 
   * @param pool
   *          the pool this thread works in
   * @throws NullPointerException
   *           if pool is null
   */
  @SuppressWarnings("unchecked")
  protected WorkStealingQueue(boolean locallyFifo, WorkStealing<T> pool) {
    if (pool == null)
      throw new NullPointerException();
    this.pool = pool;
    this.locallyFifo = locallyFifo;
    int rs = seedGenerator.nextInt();
    seed = rs == 0 ? 1 : rs; // seed must be nonzero

    queue = (T[]) new Object[INITIAL_QUEUE_CAPACITY];
  }

  /*
   * Intrinsics-based atomic writes for queue slots. These are basically the
   * same as methods in AtomicObjectArray, but specialized for (1) ForkJoinTask
   * elements (2) requirement that nullness and bounds checks have already been
   * performed by callers and (3) effective offsets are known not to overflow
   * from int to long (because of MAXIMUM_QUEUE_CAPACITY). We don't need
   * corresponding version for reads: plain array reads are OK because they
   * protected by other volatile reads and are confirmed by CASes.
   * 
   * Most uses don't actually call these methods, but instead contain inlined
   * forms that enable more predictable optimization. We don't define the
   * version of write used in pushTask at all, but instead inline there a
   * store-fenced array slot write.
   */

  /**
   * CASes slot i of array q from t to null. Caller must ensure q is non-null
   * and index is in range.
   */
  private static final <T> boolean casSlotNull(T[] q, int i, T t) {
    return UNSAFE.compareAndSwapObject(q, (i << qShift) + qBase, t, null);
  }

  /**
   * Performs a volatile write of the given task at given slot of array q.
   * Caller must ensure q is non-null and index is in range. This method is used
   * only during resets and backouts.
   */
  private static final <T> void writeSlot(T[] q, int i, T t) {
    UNSAFE.putObjectVolatile(q, (i << qShift) + qBase, t);
  }

  /**
   * Pushes a task. Call only from this thread.
   * 
   * @param t
   *          the task. Caller must ensure non-null.
   */
  public final void pushTask(T t) {
    T[] q = queue;
    int mask = q.length - 1; // implicit assert q != null
    int s = sp++; // ok to increment sp before slot write
    UNSAFE.putOrderedObject(q, ((s & mask) << qShift) + qBase, t);
    if ((s -= base) == 0)
      pool.signalWork(); // was empty
    else if (s == mask)
      growQueue(); // is full
  }

  /**
   * Doubles queue array size. Transfers elements by emulating steals (deqs)
   * from old array and placing, oldest first, into new array.
   */
  @SuppressWarnings("unchecked")
  private void growQueue() {
    T[] oldQ = queue;
    int oldSize = oldQ.length;
    int newSize = oldSize << 1;
    if (newSize > MAXIMUM_QUEUE_CAPACITY)
      throw new RejectedExecutionException("Queue capacity exceeded");
    T[] newQ = queue = (T[]) new Object[newSize];

    int b = base;
    int bf = b + oldSize;
    int oldMask = oldSize - 1;
    int newMask = newSize - 1;
    do {
      int oldIndex = b & oldMask;
      T t = oldQ[oldIndex];
      if (t != null && !casSlotNull(oldQ, oldIndex, t))
        t = null;
      writeSlot(newQ, b & newMask, t);
    } while (++b != bf);
    pool.signalWork();
  }

  /**
   * Find and execute tasks and check status while running
   */
  public final T poll() {
    T t; // try to get and run stolen or submitted task
    if (foundLocal) {
      t = getLocalItem();
    } else if ((t = scan()) != null) {
      if (base != sp)
        foundLocal = true;
    }
    if (t == null) {
      pool.preStep(this, prevRan);
      prevRan = false;
    } else {
      prevRan = true;
    }

    return t;
  }

  /**
   * Runs local tasks until queue is empty or shut down. Call only while active.
   */
  private T getLocalItem() {
    while (true) {
      T t = locallyFifo ? locallyDeqTask() : popTask();
      if (t != null) {
        return t;
      } else if (base == sp) {
        foundLocal = false;
        return null;
      }
    }
  }

  /**
   * Tries to take a task from the base of own queue. Assumes active status.
   * Called only by current thread.
   * 
   * @return a task, or null if none
   */
  private T locallyDeqTask() {
    T[] q = queue;
    if (q != null) {
      T t;
      int b, i;
      while (sp != (b = base)) {
        if ((t = q[i = (q.length - 1) & b]) != null && UNSAFE.compareAndSwapObject(q, (i << qShift) + qBase, t, null)) {
          base = b + 1;
          return t;
        }
      }
    }
    return null;
  }

  /**
   * Tries to take a task from the base of the queue, failing if empty or
   * contended. Note: Specializations of this code appear in scan and
   * scanWhileJoining.
   * 
   * @return a task, or null if none or contended
   */
  final T deqTask() {
    T t;
    T[] q;
    int b, i;
    if ((b = base) != sp && (q = queue) != null && // must read q after b
        (t = q[i = (q.length - 1) & b]) != null && UNSAFE.compareAndSwapObject(q, (i << qShift) + qBase, t, null)) {
      base = b + 1;
      return t;
    }
    return null;
  }

  /**
   * Returns a popped task, or null if empty. Assumes active status. Called only
   * by current thread. (Note: a specialization of this code appears in
   * popWhileJoining.)
   */
  private T popTask() {
    int s;
    T[] q;
    if (base != (s = sp) && (q = queue) != null) {
      int i = (q.length - 1) & --s;
      T t = q[i];
      if (t != null && UNSAFE.compareAndSwapObject(q, (i << qShift) + qBase, t, null)) {
        sp = s;
        return t;
      }
    }
    return null;
  }

  /**
   * Tries to steal a task from another worker. Starts at a random index of
   * workers array, and probes workers until finding one with non-empty queue or
   * finding that all are empty. It randomly selects the first n probes. If
   * these are empty, it resorts to a circular sweep, which is necessary to
   * accurately set active status. (The circular sweep uses steps of
   * approximately half the array size plus 1, to avoid bias stemming from
   * leftmost packing of the array in ForkJoinPool.)
   * 
   * This method must be both fast and quiet -- usually avoiding memory accesses
   * that could disrupt cache sharing etc other than those needed to check for
   * and take tasks (or to activate if not already active). This accounts for,
   * among other things, updating random seed in place without storing it until
   * exit.
   * 
   * @return a task, or null if none found
   */
  private T scan() {
    WorkStealing<T> p = pool;
    WorkStealingQueue<T>[] ws; // worker array
    int n; // upper bound of #workers
    if ((ws = p.workers) != null && (n = ws.length) > 1) {
      boolean canSteal = active; // shadow active status
      int r = seed; // extract seed once
      int mask = n - 1;
      int j = -n; // loop counter
      int k = r; // worker index, random if j < 0
      for (;;) {
        WorkStealingQueue<T> v = ws[k & mask];
        r ^= r << 13;
        r ^= r >>> 17;
        r ^= r << 5; // inline xorshift
        if (v != null && v.base != v.sp) {
          int b, i; // inline specialized deqTask
          T[] q;
          T t;
          if ((canSteal || // ensure active status
              (canSteal = active = p.tryIncrementActiveCount()))
              && (q = v.queue) != null
              && (t = q[i = (q.length - 1) & (b = v.base)]) != null
              && UNSAFE.compareAndSwapObject(q, (i << qShift) + qBase, t, null)) {
            v.base = b + 1;
            seed = r;
            ++stealCount;
            return t;
          }
          j = -n;
          k = r; // restart on contention
        } else if (++j <= 0)
          k = r;
        else if (j <= n)
          k += (n >>> 1) | 1;
        else
          break;
      }
    }
    return null;
  }

  // Unsafe mechanics

  private static final sun.misc.Unsafe UNSAFE = getUnsafe();
  private static final long qBase = UNSAFE.arrayBaseOffset(Object[].class);
  private static final int qShift;

  static {
    int s = UNSAFE.arrayIndexScale(Object[].class);
    if ((s & (s - 1)) != 0)
      throw new Error("data type scale not a power of two");
    qShift = 31 - Integer.numberOfLeadingZeros(s);
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
