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
import galois.objects.MethodFlag;
import galois.runtime.wl.OrderedWorklist;
import galois.runtime.wl.ParameterOrderedWorklist;
import galois.runtime.wl.ParameterUnorderedWorklist;
import galois.runtime.wl.ParameterWorklist;
import galois.runtime.wl.Priority;
import galois.runtime.wl.Priority.Rule;
import galois.runtime.wl.UnorderedWorklist;
import galois.runtime.wl.Worklists;

import java.io.FileInputStream;
import java.io.PrintStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import util.Launcher;
import util.Reflection;
import util.RuntimeStatistics;
import util.Sampler;
import util.StackSampler;
import util.Statistics;
import util.SystemProperties;
import util.fn.Lambda0Void;
import util.fn.Lambda2Void;
import util.fn.Lambda3Void;
import util.fn.LambdaVoid;

/**
 * Provides methods to access Galois runtime from application code.
 * 
 */
public final class GaloisRuntime {
  private static final boolean USE_ORDERED_V2 = SystemProperties.getBooleanProperty("usev2", false);
  private static final int ITERATION_MULTIPLIER = SystemProperties.getIntProperty("iterationMultiplier", 1);
  private static Logger logger = Logger.getLogger("galois.runtime.GaloisRuntime");
  private static GaloisRuntime instance = null;

  private boolean invalid;
  private final boolean useParameter;
  private final boolean useSerial;
  private final int maxThreads;

  private final int maxIterations;
  private final boolean moreStats;
  private final boolean ignoreUserFlags;

  private final ArrayDeque<ExecutorFrame> stack;
  private final Executor root;
  private ExecutorFrame current;
  private final ThreadPool pool;
  private static byte currentMask;

  private GaloisRuntime(ThreadPool pool, int numThreads, boolean useParameter, boolean useSerial, boolean moreStats,
      boolean ignoreUserFlags) {
    this.maxIterations = ITERATION_MULTIPLIER * numThreads;
    this.maxThreads = useParameter ? 1 : numThreads;
    this.useParameter = useParameter;
    this.useSerial = useSerial;
    this.moreStats = moreStats;
    this.ignoreUserFlags = ignoreUserFlags;
    this.pool = pool;

    stack = new ArrayDeque<ExecutorFrame>();
    root = new DummyExecutor();
    current = new ExecutorFrame(root, null);
    currentMask = current.mask;
  }

  /**
   * Called by the testing framework to reset the runtime.
   */
  private static void initialize(ThreadPool pool, int numThreads, boolean useParameter, boolean useSerial,
      boolean moreStats, boolean ignoreUserFlags) {
    if (instance != null) {
      instance.invalidate();
    }

    instance = new GaloisRuntime(pool, numThreads, useParameter, useSerial, moreStats, ignoreUserFlags);
  }

  /**
   * Returns the current instance of the runtime.
   * 
   * @return a reference to the current runtime
   */
  public static GaloisRuntime getRuntime() {
    if (instance == null) {
      // Use default serial Runtime
      logger.warning("Using default serial runtime");
      initialize(null, 1, false, true, false, false);
    }
    return instance;
  }

  /**
   * Creates an unordered Galois iterator that concurrently applies a function
   * over all elements in some initial collection. Additional elements may be
   * added during iteration.
   * 
   * @param <T>
   *          type of elements to iterate over
   * @param initial
   *          initial elements to iterate over
   * @param body
   *          function to apply
   * @param priority
   *          specification of the order elements are processed
   * @throws ExecutionException
   *           if there is an uncaught exception during execution
   * @see #forall(Mappable, LambdaVoid)
   * @see #foreach(Mappable, Lambda2Void, galois.runtime.wl.Priority.Rule)
   */
  public static <T> void foreach(Iterable<T> initial, Lambda2Void<T, ForeachContext<T>> body, Rule priority)
      throws ExecutionException {
    getRuntime().checkValidity();
    getRuntime().runForeach(initial, body, priority);
  }

  /**
   * Creates an ordered Galois iterator that concurrently applies a function
   * over all elements in some initial collection. Additional elements may be
   * added during iteration. Elements are processed strictly according to some
   * order.
   * 
   * @param <T>
   *          type of elements to iterate over
   * @param initial
   *          initial elements to iterate over
   * @param body
   *          function to apply
   * @param priority
   *          specification of the order elements are processed
   * @throws ExecutionException
   *           if there is an uncaught exception during execution
   * @see #foreachOrdered(Mappable, Lambda2Void,
   *      galois.runtime.wl.Priority.Rule)
   */
  public static <T> void foreachOrdered(Iterable<T> initial, Lambda2Void<T, ForeachContext<T>> body, Rule order)
      throws ExecutionException {
    foreachOrdered(initial, body, order, null);
  }

  public static <T> void foreachOrdered(Iterable<T> initial, Lambda2Void<T, ForeachContext<T>> body, Rule order,
      Rule priority) throws ExecutionException {
    getRuntime().checkValidity();
    getRuntime().runForeachOrdered(initial, body, order, priority);
  }

  /**
   * Creates an unordered Galois iterator that concurrently applies a function
   * over all elements in some initial collection. In contrast to
   * {@link #foreach(Mappable, Lambda2Void, galois.runtime.wl.Priority.Rule)},
   * no additional elements may be added during iteration and the particular
   * order elements are processed in is dictated by the particular
   * {@link Mappable} instance.
   * 
   * @param <T>
   *          type of elements to iterate over
   * @param initial
   *          initial elements to iterate over
   * @param body
   *          function to apply
   * @throws ExecutionException
   *           if there is an uncaught exception during execution
   * @see #foreach(Mappable, Lambda2Void, Object)
   * @see #foreach(Mappable, Lambda3Void, Object, Object)
   */
  public static <T> void forall(Mappable<T> initial, LambdaVoid<T> body) throws ExecutionException {
    getRuntime().checkValidity();
    getRuntime().runForall(initial, body);
  }

  private void invalidate() {
    assert stack.isEmpty();
    // pool.shutdown();
    invalid = true;
  }

  private void checkValidity() {
    assert !invalid;
  }

  public void onCommit(Iteration it, Lambda0Void action) {
    checkValidity();
    current.executor.onCommit(it, action);
  }

  public void onUndo(Iteration it, Lambda0Void action) {
    checkValidity();
    current.executor.onUndo(it, action);
  }

  public void onRelease(Iteration it, ReleaseCallback action) {
    checkValidity();
    current.executor.onRelease(it, action);
  }

  public static boolean needMethodFlag(byte flags, byte option) {
    // Since this is called very often, skip the validity check
    // checkValidity();

    // Apparently the following check even when converted to using static
    // fields is slow
    // if (useParameter)
    // flags = MethodFlag.ALL;
    return ((flags & currentMask) & option) != 0;
  }

  <T> void callAll(List<? extends Callable<T>> callables) throws InterruptedException, ExecutionException {
    checkValidity();
    pool.callAll(callables);
  }

  /**
   * Gets the maximum number of threads that can be used by the Runtime.
   * 
   * @return maximum number of threads available
   */
  public int getMaxThreads() {
    checkValidity();
    return maxThreads;
  }

  public int getMaxIterations() {
    checkValidity();
    return maxIterations;
  }

  public int getThreadId() {
    // XXX(ddn): make less of a hack
    return current.mask == MethodFlag.NONE ? 0 : Iteration.getCurrentIteration().getId();
  }

  /**
   * Signals that conflict has been detected by the user/library code.
   * 
   * @param it
   *          the current iteration
   * @param conflicter
   *          the iteration that is in conflict with the current iteration
   */
  public void raiseConflict(Iteration it, Iteration conflicter) {
    checkValidity();
    current.executor.arbitrate(it, conflicter);
  }

  private void push(ExecutorFrame frame) {
    stack.push(current);
    current = frame;
    currentMask = current.mask;
  }

  private void pop() {
    current = stack.pop();
    currentMask = current.mask;
  }

  void replaceWithRootContextAndCall(final Lambda0Void callback) throws ExecutionException {
    ExecutorFrame oldFrame = current;

    pop();
    try {
      Callable<IterationStatistics> callable = new Callable<IterationStatistics>() {
        @Override
        public IterationStatistics call() throws Exception {
          callback.call();
          return null;
        }
      };
      pushContextAndCall(new ExecutorFrame(root, callable));
    } finally {
      push(oldFrame);
    }
  }

  private IterationStatistics __stackSamplerRecordMe(Callable<IterationStatistics> callback) throws Exception {
    return callback.call();
  }

  private IterationStatistics pushContextAndCall(ExecutorFrame frame) throws ExecutionException {
    push(frame);
    try {
      if (frame.executor.isSerial())
        return __stackSamplerRecordMe(frame.callback);
      else
        return frame.callback.call();
    } catch (Exception e) {
      throw new ExecutionException(e);
    } finally {
      pop();
    }
  }

  private <T, S> void initializeWorklist(final ParameterWorklist<T, S> wl, Iterable<T> initial)
      throws ExecutionException {
    for (T item : initial) {
      wl.add(item);
    }
  }

  private <T, S> void initializeWorklist(final ParameterWorklist<T, S> wl, Mappable<T> initial) {
    initial.map(new LambdaVoid<T>() {
      @Override
      public void call(T item) {
        wl.add(item);
      }
    });
  }

  private <T> ExecutorFrame makeForeachFrame(Iterable<T> initial, final Lambda2Void<T, ForeachContext<T>> body,
      Rule priority) throws ExecutionException {
    Callable<IterationStatistics> callable;
    Executor next;
    if (useSerial) {
      final UnorderedWorklist<T> wl = Priority.makeUnordered(priority, true, null);
      final SerialExecutor<T> ex = new SerialExecutor<T>();
      Worklists.initialWorkDistribution(wl, initial, maxThreads);

      callable = new Callable<IterationStatistics>() {
        @Override
        public IterationStatistics call() throws Exception {
          return ex.call(body, wl);
        }
      };
      next = ex;
    } else if (useParameter) {
      final ParameterUnorderedWorklist<T> wl = Priority.makeParameterUnordered();
      final AbstractParameterExecutor<T, T> ex = new ParameterUnorderedExecutor<T>(wl);
      initializeWorklist(wl, initial);
      callable = new Callable<IterationStatistics>() {
        @Override
        public IterationStatistics call() throws Exception {
          return ex.call(body);
        }
      };
      next = ex;
    } else {
      final UnorderedWorklist<T> wl = Priority.makeUnordered(priority, false, null);
      final UnorderedExecutor<T> ex = new UnorderedExecutor<T>();
      Worklists.initialWorkDistribution(wl, initial, maxThreads);
      callable = new Callable<IterationStatistics>() {
        @Override
        public IterationStatistics call() throws Exception {
          return ex.call(body, wl);
        }
      };
      next = ex;
    }

    return new ExecutorFrame(next, callable);
  }

  private <T> ExecutorFrame makeForeachOrderedFrame(Iterable<T> initial, final Lambda2Void<T, ForeachContext<T>> body,
      Rule order, Rule priority) throws ExecutionException {
    return USE_ORDERED_V2 ? makeForeachOrderedFrame2(initial, body, order, priority) : makeForeachOrderedFrame1(
        initial, body, order);

  }

  private <T> ExecutorFrame makeForeachOrderedFrame1(Iterable<T> initial, final Lambda2Void<T, ForeachContext<T>> body,
      Rule priority) throws ExecutionException {
    Callable<IterationStatistics> callable;
    Executor next;
    if (useSerial) {
      final UnorderedWorklist<T> wl = Priority.makeOrdered(priority, true);
      final SerialExecutor<T> ex = new SerialExecutor<T>();
      Worklists.initialWorkDistribution(wl, initial, maxThreads);
      callable = new Callable<IterationStatistics>() {
        @Override
        public IterationStatistics call() throws Exception {
          return ex.call(body, wl);
        }
      };
      next = ex;
    } else if (useParameter) {
      final ParameterOrderedWorklist<T> wl = Priority.makeParameterOrdered(priority);
      final ParameterOrderedExecutor<T> ex = new ParameterOrderedExecutor<T>(wl);
      initializeWorklist(wl, initial);
      callable = new Callable<IterationStatistics>() {
        @Override
        public IterationStatistics call() throws Exception {
          return ex.call(body);
        }
      };
      next = ex;

    } else {
      final OrderedWorklist<T> wl = Priority.makeOrdered(priority, false);
      final OrderedExecutor<T> ex = new OrderedExecutor<T>(wl);
      Worklists.initialWorkDistribution(wl, initial, maxThreads);
      callable = new Callable<IterationStatistics>() {
        @Override
        public IterationStatistics call() throws Exception {
          return ex.call(body, wl);
        }
      };
      next = ex;
    }
    return new ExecutorFrame(next, callable);
  }

  private <T> ExecutorFrame makeForeachOrderedFrame2(final Iterable<T> initial,
      final Lambda2Void<T, ForeachContext<T>> body, final Rule order, final Rule priority) throws ExecutionException {
    Callable<IterationStatistics> callable;
    Executor next;
    if (useSerial) {
      final UnorderedWorklist<T> wl = Priority.makeOrdered(order, true);
      final SerialExecutor<T> ex = new SerialExecutor<T>();
      Worklists.initialWorkDistribution(wl, initial, maxThreads);
      callable = new Callable<IterationStatistics>() {
        @Override
        public IterationStatistics call() throws Exception {
          return ex.call(body, wl);
        }
      };
      next = ex;
    } else if (useParameter) {
      final ParameterOrderedWorklist<T> wl = Priority.makeParameterOrdered(order);
      final ParameterOrderedExecutor<T> ex = new ParameterOrderedExecutor<T>(wl);
      initializeWorklist(wl, initial);
      callable = new Callable<IterationStatistics>() {
        @Override
        public IterationStatistics call() throws Exception {
          return ex.call(body);
        }
      };
      next = ex;

    } else {
      final OrderedV2Executor<T> ex = new OrderedV2Executor<T>();
      callable = new Callable<IterationStatistics>() {
        @Override
        public IterationStatistics call() throws Exception {
          return ex.call(body, initial, order, priority);
        }
      };
      next = ex;
    }
    return new ExecutorFrame(next, callable);
  }

  private <T> ExecutorFrame makeForallFrame(final Mappable<T> mappable, final LambdaVoid<T> body)
      throws ExecutionException {
    Executor next;
    Callable<IterationStatistics> callable;
    if (useSerial) {
      final SerialPmapExecutor<T> ex = new SerialPmapExecutor<T>(mappable);
      callable = new Callable<IterationStatistics>() {
        @Override
        public IterationStatistics call() throws Exception {
          return ex.call(body);
        }
      };
      next = ex;
    } else if (useParameter) {
      ParameterUnorderedWorklist<T> wl = Priority.makeParameterUnordered();
      initializeWorklist(wl, mappable);
      final ParameterUnorderedExecutor<T> ex = new ParameterUnorderedExecutor<T>(wl);
      callable = new Callable<IterationStatistics>() {
        @Override
        public IterationStatistics call() throws Exception {
          return ex.call(body);
        }
      };
      next = ex;
    } else {
      final PmapExecutor<T> ex = new PmapExecutor<T>();
      callable = new Callable<IterationStatistics>() {
        @Override
        public IterationStatistics call() throws Exception {
          return ex.call(mappable, body);
        }
      };
      next = ex;
    }
    return new ExecutorFrame(next, callable);
  }

  private <T> void runForeach(Iterable<T> initial, final Lambda2Void<T, ForeachContext<T>> body, Rule priority)
      throws ExecutionException {
    if (!current.executor.isSerial())
      throw new Error("nesting support disabled");

    if (current.executor instanceof PmapExecutor)
      throw new Error("Not yet supported");

    // current.executor.suspend(pool);

    IterationStatistics stats = pushContextAndCall(makeForeachFrame(initial, body, priority));
    Launcher.getLauncher().addStats(stats);

  }

  private <T> void runForeachOrdered(Iterable<T> initial, final Lambda2Void<T, ForeachContext<T>> body, Rule order,
      Rule priority) throws ExecutionException {
    if (!current.executor.isSerial())
      throw new Error("nesting support disabled");

    if (current.executor instanceof PmapExecutor)
      throw new Error("Not yet supported");

    // current.executor.suspend(pool);

    IterationStatistics stats = pushContextAndCall(makeForeachOrderedFrame(initial, body, order, priority));
    Launcher.getLauncher().addStats(stats);

  }

  private <T> void runForall(Mappable<T> mappable, LambdaVoid<T> body) throws ExecutionException {
    if (!current.executor.isSerial()) {
      throw new Error("nesting support disabled");
    }

    // NB(ddn): Can lead to deadlock (infinite livelock) when using mappable
    // iterators because one thread sleeps holding its locks and the other
    // threads keep executing but they can never suspend their executor
    if (current.executor instanceof PmapExecutor)
      throw new Error("Not yet supported");
    // current.executor.suspend(pool);

    IterationStatistics stats = pushContextAndCall(makeForallFrame(mappable, body));
    Launcher.getLauncher().addStats(stats);
  }

  public boolean useParameter() {
    checkValidity();
    return useParameter;
  }

  public boolean useSerial() {
    checkValidity();
    return useSerial;
  }

  public boolean ignoreUserFlags() {
    checkValidity();
    return ignoreUserFlags;
  }

  public boolean inRoot() {
    return current.executor == root;
  }

  public boolean moreStats() {
    checkValidity();
    return moreStats;
  }

  private static class ExecutorFrame {
    final Callable<IterationStatistics> callback;
    final Executor executor;
    final byte mask;

    public ExecutorFrame(Executor executor, Callable<IterationStatistics> callback) {
      this.executor = executor;
      this.callback = callback;
      this.mask = executor.isSerial() ? MethodFlag.NONE : MethodFlag.ALL;
    }
  }

  private static class DummyExecutor implements Executor {
    @Override
    public void arbitrate(Iteration current, Iteration conflicter) throws IterationAbortException {
    }

    @Override
    public void onCommit(Iteration it, Lambda0Void action) {
    }

    @Override
    public void onRelease(Iteration it, ReleaseCallback action) {
    }

    @Override
    public void onUndo(Iteration it, Lambda0Void action) {
    }

    public boolean isSerial() {
      return true;
    }
  }

  private static void dumpStatistics(List<Statistics> stats, PrintStream summaryOut, PrintStream fullOut) {
    if (stats.isEmpty())
      return;

    fullOut.println("====== Individual Statistics ======");
    for (Statistics stat : stats) {
      stat.dumpFull(fullOut);
    }

    List<Statistics> merged = new ArrayList<Statistics>();
    Map<Class<? extends Statistics>, Statistics> reps = new HashMap<Class<? extends Statistics>, Statistics>();

    for (Statistics stat : stats) {
      Class<? extends Statistics> key = stat.getClass();
      Statistics rep = reps.get(key);

      if (rep == null) {
        reps.put(key, stat);
        merged.add(stat);
      } else {
        rep.merge(stat);
      }
    }

    summaryOut.println("==== Summary Statistics ====");
    fullOut.println("====== Merged Statistics ======");
    for (Statistics stat : merged) {
      stat.dumpSummary(summaryOut);
      stat.dumpFull(fullOut);
    }
  }

  private static void usage() {
    System.err.println("java -cp ... galois.runtime.GaloisRuntime [options] <main class> <args>*");
    System.err.println(" -r <num runs>      : number of runs to use");
    System.err.println(" -t <num threads>   : number of threads to use");
    System.err.println(" -f <property file> : property file to read arguments from");
    System.err.println(" -p                 : use ParaMeter");
    System.err.println(" -s                 : use serial data structures and executor");
    System.err.println(" -g                 : enable additional statistics.");
    System.err.println("                      Currently: stack profiling, processor utilization");
    System.err.println(" --help             : print help");
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    String main = null;
    String[] mainArgs = new String[0];
    boolean useParameter = false;
    boolean useSerial = false;
    boolean moreStats = false;
    boolean ignoreUserFlags = false;
    int samplerInterval = 0;
    int numThreads = 1;
    int numRuns = 1;

    for (int i = 0; i < args.length; i++) {
      String arg = args[i];
      if (arg.equals("-r")) {
        numRuns = Integer.parseInt(args[++i]);
      } else if (arg.equals("-t")) {
        numThreads = Integer.parseInt(args[++i]);
      } else if (arg.equals("-p")) {
        useParameter = true;
      } else if (arg.equals("-f")) {
        Properties p = new Properties(System.getProperties());
        p.load(new FileInputStream(args[++i]));
        System.setProperties(p);
      } else if (arg.equals("-s")) {
        useSerial = true;
      } else if (arg.equals("-g")) {
        samplerInterval = 100;
        moreStats = true;
      } else if (arg.equals("-i")) {
        ignoreUserFlags = true;
      } else if (arg.equals("--help")) {
        usage();
        System.exit(1);
      } else {
        main = arg;
        mainArgs = Arrays.asList(args).subList(i + 1, args.length).toArray(new String[args.length - i - 1]);
        break;
      }
    }

    if (main == null) {
      usage();
      System.exit(1);
    }

    if (useParameter) {
      samplerInterval = 0;
      numThreads = 1;
      numRuns = 1;
    }

    String defaultArgs = System.getProperty("args");
    if (defaultArgs != null) {
      if (mainArgs.length != 0) {
        System.err.println("'args' property and commandline args both given");
        System.exit(1);
      }
      mainArgs = defaultArgs.split("\\s+");
    }

    // Run
    RuntimeStatistics stats = new RuntimeStatistics();
    Launcher launcher = Launcher.getLauncher();

    ThreadPool pool = new ThreadPool(numThreads);
    for (int i = 0; i < numRuns; i++) {
      if (i != 0)
        launcher.reset();

      if (i == numRuns - 1)
        launcher.setLastRun();

      initialize(pool, numThreads, useParameter, useSerial, moreStats, ignoreUserFlags);

      Sampler sampler = StackSampler.start(samplerInterval);
      launcher.startTiming();
      try {
        Reflection.invokeStaticMethod(main, "main", new Object[] { mainArgs });
        launcher.stopTiming();
        launcher.addStats(sampler.stop(), true);
        long timeWithoutGc = launcher.elapsedTime(true);
        long timeWithGc = launcher.elapsedTime(false);
        if (logger.isLoggable(Level.INFO)) {
          logger.info("Runtime (ms): " + timeWithGc + " (without GC: " + timeWithoutGc + ")");
        }
        stats.putStats(timeWithoutGc, timeWithGc);
      } catch (Exception e) {
        throw e;
      }
    }
    pool.shutdown();

    launcher.addStats(stats, true);
    PrintStream out = new PrintStream("stats.txt");
    dumpStatistics(launcher.getStatistics(), System.out, out);
    out.close();
  }
}
