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

package galois.runtime.wl;

import util.fn.Lambda;
import util.fn.Lambda0;

@MatchingConcurrentVersion(ConcurrentOrderedByFloatMetric.class)
@MatchingLeafVersion(OrderedByFloatMetricLeaf.class)
@FunctionParameter(FunctionParameterType.LAMBDA_T_FLOAT_1)
public class OrderedByFloatMetric<T> extends OrderedByIntegerMetric<T> {
  public static final int DEFAULT_NUM_BUCKETS = 1024;
  
  /**
   * Creates an ascending order
   * 
   * @param indexer     function mapping elements to integers 
   */
  public OrderedByFloatMetric(float range, Lambda<T, Float> indexer, Lambda0<Worklist<T>> maker, boolean needSize) {
    this(range, indexer, false, DEFAULT_NUM_BUCKETS, false, maker, needSize);
  }

  /**
   * Creates an ascending or descending order
   * 
   * @param descending   true if descending order, otherwise ascending
   * @param indexer     function mapping elements to integers 
   */
  public OrderedByFloatMetric(final float range, final Lambda<T, Float> indexer, boolean descending, final int numBuckets, boolean approxMonotonic, Lambda0<Worklist<T>> maker, boolean needSize) {
    super(numBuckets, new Lambda<T,Integer>() {
      final float scale = numBuckets / range;
      @Override
      public Integer call(T arg0) {
        float v = indexer.call(arg0) * scale;
        return (int) v;
      }
    }, descending, approxMonotonic, maker, needSize);
  }
}
