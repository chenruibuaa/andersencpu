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

File: CounterToSuspendWith.java 

 */

package galois.objects;

import galois.runtime.AbstractForeachContext;
import galois.runtime.ForeachContext;
import galois.runtime.GaloisRuntime;
import galois.runtime.Iteration;
import util.concurrent.NotThreadSafe;
import util.fn.Lambda0Void;
import util.fn.LambdaVoid;

/**
 * Builds counters that trigger a Galois iterator to suspend with a serial
 * action when a given value is reached. After the serial action finishes,
 * execution returns to the iterator and the counter is reset. Unlike
 * {@link CounterToSuspendWithBuilder} these counters XXX.
 * 
 * @param <T>
 *          type of elements being iterated over in Galois iterator
 * @see ForeachContext#suspendWith(util.fn.Lambda0Void)
 * @see CounterToSuspendWithBuilder
 */
public class WeakCounterToSuspendWithBuilder {

  /**
   * Creates a new counter. When this counter reaches the given value or more,
   * suspend the current Galois iterator and execute the given action.
   * 
   * @param countTo
   *          value to count to
   * @param exact
   *          trigger action when value is exactly <code>countTo</code>;
   *          otherwise, trigger action when <code>value &gt;= countTo</code>
   * @param callback
   *          action to execute when counter is triggered
   */
  public <T> Counter<T> create(int countTo, LambdaVoid<ForeachContext<T>> callback) {
    Countable c = GaloisRuntime.getRuntime().useSerial() ? new SerialCounter() : new ConcurrentCounter();
    return new MyCounter<T>(countTo, callback, c);
  }

  private interface Countable {
    @NotThreadSafe
    public int addAndGet(int delta);

    public int addAndGet(int tid, int delta, int countTo);

    @NotThreadSafe
    public void reset();

    @NotThreadSafe
    public int get();
  }

  private static class MyCounter<T> implements Counter<T> {
    private final int countTo;
    private final Countable counter;
    private final LambdaVoid<ForeachContext<T>> callback;

    public MyCounter(int countTo, LambdaVoid<ForeachContext<T>> callback, Countable counter) {
      this.countTo = countTo;
      this.callback = callback;
      this.counter = counter;
    }

    @Override
    public void access(Iteration it, byte flags) {
    }

    @Override
    public void increment(ForeachContext<T> ctx, int delta, byte flags) {
      if (counter.addAndGet(ctx.getThreadId(), delta, countTo) >= countTo) {
        ctx.suspendWith(new MyCallback(ctx));
      }
    }

    @Override
    public final void increment(int delta) {
      if (counter.addAndGet(delta) >= countTo) {
        throw new IllegalArgumentException("Serial Increment should not trigger callback");
      }
    }

    @Override
    public void reset() {
      counter.reset();
    }

    @Override
    public final void increment(ForeachContext<T> ctx) {
      increment(ctx, MethodFlag.ALL);
    }

    @Override
    public final void increment(ForeachContext<T> ctx, byte flags) {
      increment(ctx, 1, flags);
    }

    @Override
    public final void increment(final ForeachContext<T> ctx, final int delta) {
      increment(ctx, delta, MethodFlag.ALL);
    }

    private class MyCallback extends AbstractForeachContext<T> implements Lambda0Void {
      private ForeachContext<T> ctx;

      public MyCallback(ForeachContext<T> ctx) {
        this.ctx = ctx;
      }

      @Override
      public final void add(T item) {
        ctx.add(item, MethodFlag.NONE);
      }

      @Override
      public void call() {
        if (counter.get() < countTo) {
          return;
        }
        reset();

        callback.call(this);
      }
    }
  }

  private static class SerialCounter implements Countable {
    private int value;

    @Override
    public int addAndGet(int delta) {
      value += delta;
      return value;
    }

    @Override
    public void reset() {
      value = 0;
    }

    @Override
    public int get() {
      return value;
    }

    @Override
    public int addAndGet(int tid, int delta, int countTo) {
      return addAndGet(delta);
    }
  }

  private static class ConcurrentCounter implements Countable {
    private static final int numThreads = GaloisRuntime.getRuntime().getMaxThreads();
    private static final int CACHE_MULTIPLE = 16;
    private final int[] values;
    private int state = 0;

    public ConcurrentCounter() {
      values = new int[numThreads * CACHE_MULTIPLE];
    }

    @Override
    public int addAndGet(int delta) {
      state = (state + 1) % numThreads;
      values[state] += delta;
      return values[state] * numThreads;
    }

    @Override
    public void reset() {
      for (int i = 0; i < numThreads; i++)
        values[i * CACHE_MULTIPLE] = 0;
    }

    @Override
    public int get() {
      int max = -1;
      for (int i = 0; i < numThreads; i++) {
        if (max < values[i * CACHE_MULTIPLE])
          max = values[i * CACHE_MULTIPLE];
      }
      return max * numThreads;
    }

    @Override
    public int addAndGet(int tid, int delta, int countTo) {
      values[tid * CACHE_MULTIPLE] += delta;
      return values[tid * CACHE_MULTIPLE] * numThreads;
    }
  }
}
