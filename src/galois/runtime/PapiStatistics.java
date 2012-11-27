package galois.runtime;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import util.Papi;
import util.Statistics;

class PapiStatistics extends Statistics {
  private static final String[] events =
    new String[] { "PAPI_TOT_INS", "PAPI_TOT_CYC", "PAPI_L2_DCM"};
  private static final int CACHE_MULTIPLE = 8;
  private static final int WINDOW = 1024;
  private final int[] window;
  private List<long[]> counters;
  private final List<Stats> accum;
  
  public PapiStatistics(int numThreads) {
    accum = new ArrayList<Stats>(numThreads);
    for (int i = 0; i < numThreads; i++)
      accum.add(new Stats());
    window = new int[numThreads * CACHE_MULTIPLE];
    
    int numEvents = Papi.startSystem(events);
    if (numEvents < events.length)
      throw new Error("Couldn't load all event counters");
    counters = new ArrayList<long[]>(numThreads);
    for (int i = 0; i < numThreads; i++)
      counters.add(new long[numEvents]);
  }

  private void printResults(PrintStream out) {
    double ins = 0;
    double cyc = 0;
    double dcm = 0;
    
    for (Stats stats : accum) {
      ins += stats.ins;
      cyc += stats.cyc;
      dcm += stats.dcm;
    }
    
    double cpi = ins != 0 ? cyc / ins : 0.0;
    double mpi = ins != 0 ? dcm / ins : 0.0;
    out.printf("cycles: %.3f l2 misses: %.4f\n", cpi, mpi);
  }
  
  @Override
  public void dumpSummary(PrintStream out) {
    printSummaryHeader(out, "Papi (per instruction)");
    printResults(out);
  }

  @Override
  public void dumpFull(PrintStream out) {
    printFullHeader(out, "Papi (per instruction)");
    printResults(out);
  }

  @Override
  protected void doMerge(Object other) {
    PapiStatistics c = (PapiStatistics) other;
    accum.addAll(c.accum);
  }

  private static int getIndex(int tid) {
    return tid * CACHE_MULTIPLE;
  }

  public Statistics finish() {
    Papi.finishSystem();
    return this;
  }

  public void finishThread(int tid) {
    Papi.finishThread(tid);
  }

  public void startThread(int tid) {
    Papi.startThread(tid);
  }

  public static boolean isLoaded() {
    return Papi.isLoaded();
  }

  public void putStats(int tid) {
    int index = getIndex(tid);
    
    if (window[index]++ >= WINDOW) {
      long[] values = counters.get(tid);
      Papi.readThread(tid, values);
      Stats stats = accum.get(tid);
      stats.ins += values[0];
      stats.cyc += values[1];
      stats.dcm += values[2];
      window[index] = 0;
    }
  }
  private static class Stats {
    long cyc;
    long ins;
    long dcm;
  }
}
