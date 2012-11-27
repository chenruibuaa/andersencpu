package galois.runtime.wl;

import galois.runtime.ThreadContext;

public class Worklists {
  public static <T> void initialWorkDistribution(Worklist<T> wl, Iterable<T> initial, int numThreads) {
    ThreadContext ctx = new RoundRobinContext(numThreads);
//    ThreadContext ctx = new BlockedContext(numThreads, 1024);

    for (T item : initial) {
      wl.add(item, ctx);
    }
  }
  
  private static class BlockedContext implements ThreadContext {
    private final int maxThreads;
    private final int blockSize;
    private int current;

    public BlockedContext(int maxThreads, int blockSize) {
      this.maxThreads = maxThreads;
      this.blockSize = blockSize;
    }

    @Override
    public int getThreadId() {
      return (current++ / blockSize) % maxThreads;
    }
  }
  
  private static class RoundRobinContext implements ThreadContext {
    private final int maxThreads;
    private int current;

    public RoundRobinContext(int maxThreads) {
      this.maxThreads = maxThreads;
    }

    @Override
    public int getThreadId() {
      int retval = current;
      if (++current >= maxThreads) {
        current = 0;
      }
      return retval;
    }
  }
}
