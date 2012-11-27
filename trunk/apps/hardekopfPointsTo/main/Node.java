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

 File: Node.java
 */

package hardekopfPointsTo.main;

import galois.objects.GObject;
import util.fn.Lambda2Void;
import util.fn.Lambda3Void;
import util.fn.Lambda4Void;
import util.fn.LambdaVoid;

public interface Node extends GObject {

  // used by the multigraph, not to be used directly
  <T1 extends Node> void ___map(Lambda3Void<Integer, LambdaVoid<T1>, Byte> fn1, LambdaVoid<T1> fn2, byte domain,
      byte flags);

  <T1 extends Node, T2> void ___map(Lambda4Void<Integer, Lambda2Void<T1, T2>, T2, Byte> fn1, Lambda2Void<T1, T2> fn2,
      T2 arg2, byte domain, byte flags);

  <T1 extends Node> void ___map(Lambda3Void<Long, Lambda2Void<Integer, T1>, Byte> fn1, Lambda2Void<Integer, T1> fn2,
      byte domain, byte flags);

  // used by the multigraph, not to be used directly
  boolean ___addNeighbor(int n, byte domain);

  boolean ___addNeighbor(int n, int edgeTag, byte domain);
}
