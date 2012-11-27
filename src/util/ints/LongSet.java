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

 File: LongSet.java
 */
package util.ints;

import util.fn.Lambda2Void;
import util.fn.Lambda3Void;
import util.fn.Lambda4Void;
import util.fn.LambdaVoid;

public interface LongSet {

  public boolean add(long x);

  public boolean remove(long x);

  public boolean addAll(LongSet longSet);

  public void clear();

  public boolean contains(long n);

  public boolean isEmpty();

  public int size();

  /**
   * Applies a function to each element of this mappable instance serially.
   *
   * @param body function to apply to each element
   */
  public void map(LambdaVoid<Long> body);

  /**
   * Applies a function to each element of this mappable instance serially.
   *
   * @param body function to apply to each element
   * @param arg1 additional argument to function
   */
  public <A1> void map(Lambda2Void<Long, A1> body, A1 arg1);

  /**
   * Applies a function to each element of this mappable instance serially.
   *
   * @param body function to apply to each element
   * @param arg1 additional argument to function
   * @param arg2 additional argument to function
   */
  public <A1, A2> void map(Lambda3Void<Long, A1, A2> body, A1 arg1, A2 arg2);

  public <A1, A2, A3> void map(Lambda4Void<Long, A1, A2, A3> body, A1 arg1, A2 arg2, A3 arg3);

}
