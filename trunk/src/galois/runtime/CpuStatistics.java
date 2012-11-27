package galois.runtime;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import util.CPUFunctions;
import util.Statistics;

class CpuStatistics extends Statistics {
  private static final int MAX_CPU_ID = 256;
  private List<List<Integer>> results;
  private final List<int[]> cpuIds;

  public CpuStatistics(int numThreads) {
    results = new ArrayList<List<Integer>>();
    cpuIds = new ArrayList<int[]>(numThreads);
    for (int i = 0; i < numThreads; i++)
      cpuIds.add(new int[MAX_CPU_ID]);
  }

  private void computeResults() {
    results.add(getResults());
  }

  @Override
  public void dumpFull(PrintStream out) {
    computeResults();
    printFullHeader(out, "Processor Utilization");

    out.print("Max util per logical processor [cpuid, tid, utilization, (max / total samples)] per executor:\n");
    for (List<Integer> r : results) {
      int size = r.size();
      out.print("[");
      for (int i = 0; i < size; i += 4) {
        int cpuid = r.get(i);
        int tid = r.get(i + 1);
        int max = r.get(i + 2);
        int total = r.get(i + 3);
        float average = max / (float) total;
        out.printf("%d %d %.4f (%d / %d)", cpuid, tid, average, max, total);
        if (i != size - 4) {
          out.print(", ");
        }
      }
      out.print("]");
      out.println();
    }
  }

  @Override
  public void dumpSummary(PrintStream out) {
    computeResults();
    printSummaryHeader(out, "Processor Utilization");

    int max = 0;
    int sum = 0;
    int count = 0;
    for (List<Integer> r : results) {
      for (int i = 0; i < r.size(); i += 4) {
        max += r.get(i + 2);
        sum += r.get(i + 3);
      }
      count += r.size() / 4;
    }
    float mean = sum == 0 ? 0 : max / (float) sum;
    float meanCounts = results.size() == 0 ? 0 : count / (float) results.size();

    out.printf("mean: %.4f mean total processors: %.2f\n", mean, meanCounts);
  }

  /**
   * Computes the maximum (by one thread) and total samples per cpu.
   * 
   * <p>
   * Takes data in the following form:
   * 
   * <pre>
   * ... cpu id ...
   * tid1: 1   2   0 100   1   2   3
   * tid2: 100 0   1   0   0   0   0
   * </pre>
   * 
   * and computes the maximums and total samples by column, and returns a list
   * of (cpuid, tid, max, total).
   * </p>
   * 
   * @return the result as a list of quads (cpuid, tid, max, total)
   */
  private List<Integer> getResults() {
    List<Integer> results = new ArrayList<Integer>();

    int size = cpuIds.size();
    for (int i = 0; i < MAX_CPU_ID; i++) {
      int max = 0;
      int maxTid = -1;
      int sum = 0;

      for (int j = 0; j < size; j++) {
        int value = cpuIds.get(j)[i];
        if (max < value) {
          max = value;
          maxTid = j;
        }

        sum += value;
      }

      if (sum > 0) {
        results.add(i);
        results.add(maxTid);
        results.add(max);
        results.add(sum);
      }
    }

    return results;
  }

  @Override
  protected void doMerge(Object other) {
    CpuStatistics stats = (CpuStatistics) other;
    stats.computeResults();
    results.addAll(stats.results);
  }

  public final void putStats(int tid) {
    int cpuid = CPUFunctions.getCpuId();
    cpuIds.get(tid)[cpuid]++;
  }

  public static boolean isLoaded() {
    return CPUFunctions.isLoaded();
  }
}
