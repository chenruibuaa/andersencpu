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

import java.io.PrintStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import util.CollectionMath;
import util.Statistics;

class IterationStatistics extends Statistics {
  private final Map<Thread, Long> committed;
  private final Map<Thread, Long> aborted;

  public IterationStatistics() {
    committed = new HashMap<Thread, Long>();
    aborted = new HashMap<Thread, Long>();
  }

  @Override
  public void dumpFull(PrintStream out) {
    printFullHeader(out, "Iterations");

    Collection<Long> total = total();
    Collection<Long> c = committed.values();
    Collection<Float> abortRatio = ratioLongs(aborted.values(), total);

    long sumCommitted = CollectionMath.sumLong(c);
    long sumTotal = CollectionMath.sumLong(total);
    float sumAbortRatio = sumTotal != 0 ? (sumTotal - sumCommitted) / (float) sumTotal : 0;

    out.printf("Committed Iterations: %d per thread: %s\n", sumCommitted, c);
    summarizeLongs(out, total, "\t");
    out.printf("Total Iterations: %d per thread: %s\n", sumTotal, total);
    summarizeLongs(out, total, "\t");
    out.printf("Abort ratio: %.4f per thread: %s\n", sumAbortRatio, abortRatio);
    summarizeFloats(out, abortRatio, "\t");
  }

  private Collection<Long> total() {
    Collection<Long> total = CollectionMath.sumLong(committed.values(), aborted.values());
    return total;
  }

  @Override
  public void dumpSummary(PrintStream out) {
    long total = CollectionMath.sumLong(total());
    float abortRatio;
    if (total == 0)
      abortRatio = 0;
    else
      abortRatio = CollectionMath.sumLong(aborted.values()) / (float) total;

    printSummaryHeader(out, "Iterations");
    out.printf("Committed: %d Total: %d Abort Ratio: %.4f\n", CollectionMath.sumLong(committed.values()), total,
        abortRatio);
  }

  public void putStats(Thread thread, long numCommitted, long numAborted) {
    Long n = committed.get(thread);
    if (n == null)
      committed.put(thread, numCommitted);
    else
      committed.put(thread, numCommitted + n);

    n = aborted.get(thread);
    if (n == null)
      aborted.put(thread, numAborted);
    else
      aborted.put(thread, numAborted + n);
  }

  public long getNumCommitted() {
    return CollectionMath.sumLong(committed.values());
  }

  private static <K> void mergeMap(Map<K, Long> dst, Map<K, Long> src) {
    for (Map.Entry<K, Long> entry : src.entrySet()) {
      K k = entry.getKey();
      Long v = dst.get(k);
      if (v == null) {
        dst.put(k, entry.getValue());
      } else {
        dst.put(k, v + entry.getValue());
      }
    }
  }

  @Override
  protected void doMerge(Object obj) {
    IterationStatistics other = (IterationStatistics) obj;
    mergeMap(aborted, other.aborted);
    mergeMap(committed, other.committed);
  }
}
