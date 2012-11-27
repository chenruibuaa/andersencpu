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

File: AccumulatorBuilder.java 

 */

package galois.objects;

import util.fn.Lambda0Void;
import galois.runtime.GaloisRuntime;
import galois.runtime.Iteration;

/**
 * An accumulator which allows concurrent updates but not concurrent reads.
 * 
 * @see GMutableInteger
 * @see util.MutableInteger
 */
public class IntegerAccumulatorBuilder {

  public IntegerAccumulator create() {
    return create(0);
  }

  public IntegerAccumulator create(int value) {
    if (GaloisRuntime.getRuntime().useSerial()) {
      return new SerialAccumulator(value);
    } else {
      return new ConcurrentAccumulator(value);
    }
  }

  private static class SerialAccumulator implements IntegerAccumulator {
    private int value;

    public SerialAccumulator(int v) {
      this.value = v;
    }

    @Override
    public final void add(int delta) {
      add(delta, MethodFlag.ALL);
    }

    @Override
    public void add(final int delta, byte flags) {
      value += delta;
    }

    @Override
    public int get() {
      return value;
    }

    @Override
    public void set(int v) {
      value = v;
    }

    @Override
    public void access(Iteration it, byte flags) {
    }
  }

  private static class ConcurrentAccumulator implements IntegerAccumulator {
    private static final int CACHE_MULTIPLE = 16;
    private final int[] values;

    public ConcurrentAccumulator(int v) {
      this.values = new int[GaloisRuntime.getRuntime().getMaxThreads() * CACHE_MULTIPLE];
      values[0] = v;
    }

    @Override
    public final void add(int delta) {
      add(delta, MethodFlag.ALL);
    }

    private static int getIndex(int i) {
      return i * CACHE_MULTIPLE;
    }

    @Override
    public final void add(final int delta, byte flags) {
      final int tid = GaloisRuntime.getRuntime().getThreadId();

      if (GaloisRuntime.needMethodFlag(flags, MethodFlag.SAVE_UNDO)) {
        Iteration it = Iteration.getCurrentIteration();
        if (it != null)
          GaloisRuntime.getRuntime().onUndo(it, new Lambda0Void() {
            public void call() {
              values[getIndex(tid)] -= delta;
            }
          });
      }
      values[getIndex(tid)] += delta;
    }

    @Override
    public final void access(Iteration it, byte flags) {
    }

    @Override
    public final int get() {
      int retval = 0;
      for (int i = 0; i < values.length; i += CACHE_MULTIPLE)
        retval += values[i];
      return retval;
    }

    @Override
    public final void set(int v) {
      for (int i = 0; i < values.length; i += CACHE_MULTIPLE)
        values[i] = 0;
      values[0] = v;
    }
  }
}
