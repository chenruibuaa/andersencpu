package galois.runtime;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import util.CollectionMath;
import util.Statistics;

public class IdlenessStatistics extends Statistics {
  private final List<Long> idleTimes;
  private final List<Long> threadTimes;

  public IdlenessStatistics() {
    threadTimes = new ArrayList<Long>();
    idleTimes = new ArrayList<Long>();
  }

  @Override
  public void dumpFull(PrintStream out) {
    printFullHeader(out, "Idleness");

    out.printf("Thread time per measured period (thread*ms): %s\n", threadTimes);
    out.printf("Idle thread time per measured period (thread*ms): %s\n", idleTimes);
  }

  @Override
  public void dumpSummary(PrintStream out) {
    printSummaryHeader(out, "Idleness (thread*ms)");
    long totalThread = CollectionMath.sumLong(threadTimes);
    long totalIdle = CollectionMath.sumLong(idleTimes);
    out.printf("Thread Time: %d Idle Time: %d Rel. Idleness: %.4f\n", totalThread, totalIdle, totalIdle
        / (double) totalThread);
  }

  @Override
  protected void doMerge(Object obj) {
    IdlenessStatistics other = (IdlenessStatistics) obj;
    threadTimes.addAll(other.threadTimes);
    idleTimes.addAll(other.idleTimes);
  }

  public void putStats(long systemStartTime, long systemStopTime, List<? extends Idleable> processes,
      long systemAccumWait) {
    long idleTime = 0;
    for (Idleable p : processes) {
      long stopTime = p.getStopTime();

      // TODO(ddn): Figure out why this happens
      if (p.getStartTime() == 0) {
        continue;
      }

      // Finished early, use system time as our stop time
      if (p.getStopTime() == 0) {
        stopTime = systemStopTime;
      }

      idleTime += (p.getStartTime() - systemStartTime) / 1e6;
      idleTime += (systemStopTime - stopTime) / 1e6;
      idleTime += p.getAccumWait() / 1e6;
    }

    idleTime += systemAccumWait / 1e6;
    threadTimes.add((systemStopTime - systemStartTime) / (long) 1e6 * processes.size());
    idleTimes.add(idleTime);
  }

  public static interface Idleable {
    long getAccumWait();

    long getStartTime();

    long getStopTime();
  }

}