package galois.runtime;

import galois.runtime.IdlenessStatistics.Idleable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import util.Launcher;

public abstract class ProcessGroup<T extends ProcessGroup.Process> implements Iterable<T> {
  private final CpuStatistics cpuStats;
  private final PapiStatistics papiStats;
  private final IdlenessStatistics idleStats;
  private final List<T> processes;

  public ProcessGroup(int numThreads) {
    boolean useCpuStats = GaloisRuntime.getRuntime().moreStats() && CpuStatistics.isLoaded();
    boolean usePapiStats = GaloisRuntime.getRuntime().moreStats() && PapiStatistics.isLoaded();
    idleStats = new IdlenessStatistics();
    cpuStats = useCpuStats ? new CpuStatistics(numThreads) : null;
    papiStats = usePapiStats ? new PapiStatistics(numThreads) : null;
    processes = new ArrayList<T>();
    for (int i = 0; i < numThreads; i++) {
      T p = newInstance(i);
      p.init(papiStats, cpuStats);
      processes.add(p);
    }
  }

  protected abstract T newInstance(int id);

  public IterationStatistics finish() {
    Launcher.getLauncher().addStats(idleStats);

    if (papiStats != null)
      Launcher.getLauncher().addStats(papiStats.finish());

    if (cpuStats != null) {
      Launcher.getLauncher().addStats(cpuStats);
    }

    IterationStatistics stats = new IterationStatistics();
    for (T p : processes) {
      stats.putStats(p.thread, p.numCommitted, p.numAborted);
    }

    return stats;
  }

  public void run() throws ExecutionException {
    long systemStartTime = System.nanoTime();
    try {
      GaloisRuntime.getRuntime().callAll(processes);
    } catch (InterruptedException e) {
      throw new ExecutionException(e);
    } finally {
      idleStats.putStats(systemStartTime, System.nanoTime(), processes, 0);
    }
  }
  
  public Iterator<T> iterator() {
    return processes.iterator();
  }
  
  public T get(int i) {
    return processes.get(i);
  }

  public static abstract class Process implements Callable<Object>, Idleable {
    private final int id;
    private long accumWait;
    private long numCommitted;
    private long numAborted;
    private long startTime;
    private long stopTime;
    private Thread thread;
    private long waitStart;
    private PapiStatistics papiStats;
    private CpuStatistics cpuStats;
    
    protected Process(int id) {
      this.id = id;
    }

    private void init(PapiStatistics papiStats, CpuStatistics cpuStats) {
      this.papiStats = papiStats;
      this.cpuStats = cpuStats;
    }
    
    protected abstract void run() throws Exception;

    @Override
    public final Object call() throws Exception {
      thread = Thread.currentThread();
      accumWait = 0;
      stopTime = 0;
      startTime = System.nanoTime();
      if (papiStats != null)
        papiStats.startThread(id);
      try {
        run();
      } finally {
        if (papiStats != null)
          papiStats.finishThread(id);
        stopTime = System.nanoTime();
      }

      return null;
    }

    @Override
    public long getAccumWait() {
      return accumWait;
    }

    @Override
    public long getStartTime() {
      return startTime;
    }

    @Override
    public long getStopTime() {
      return stopTime;
    }

    public final int getThreadId() {
      return id;
    }

    protected final void incrementCommitted() {
      numCommitted++;
    }
    
    protected final long getCommitted() {
      return numCommitted;
    }
    
    protected final void incrementAborted() {
      numAborted++;
    }
    
    protected final void beginIteration() {
      if (papiStats != null)
        papiStats.putStats(id);
      if (cpuStats != null)
        cpuStats.putStats(id);
    }

    protected final void startWaiting() {
      waitStart = System.nanoTime();
    }

    protected final void stopWaiting() {
      accumWait += System.nanoTime() - waitStart;
    }
  }
}
