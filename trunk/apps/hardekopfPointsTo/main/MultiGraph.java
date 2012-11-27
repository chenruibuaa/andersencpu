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

 File: MultiGraph.java
 */

package hardekopfPointsTo.main;

import galois.objects.GObject;
import galois.objects.MethodFlag;
import galois.runtime.Iteration;
import util.fn.Lambda2Void;
import util.fn.Lambda3Void;
import util.fn.Lambda4Void;
import util.fn.LambdaVoid;

public class MultiGraph<T1 extends hardekopfPointsTo.main.Node> {
  private final T1[] nodes;

  public MultiGraph(T1[] nodes) {
    this.nodes = nodes;
  }

  private final Lambda3Void<Integer, LambdaVoid<T1>, Byte> ADAPTER1_1 = new Lambda3Void<Integer, LambdaVoid<T1>, Byte>() {
    @Override
    public void call(Integer i, LambdaVoid<T1> fn, Byte flags) {
      final T1 ret = getNode(i, flags);
      fn.call(ret);
    }
  };

  private Lambda4Void<Integer, Lambda2Void<T1, Object>, Object, Byte> ADAPTER_11 = new Lambda4Void<Integer, Lambda2Void<T1, Object>, Object, Byte>() {
    @Override
    public void call(Integer i, Lambda2Void<T1, Object> fn, Object arg2, Byte flags) {
      final T1 ret = getNode(i, flags);
      fn.call(ret, arg2);
    }
  };

  private final Lambda3Void<Long, Lambda2Void<Integer, T1>, Byte> ADAPTER2 = new Lambda3Void<Long, Lambda2Void<Integer, T1>, Byte>() {
    final long MASK = (1L << 32) - 1;

    @Override
    public void call(Long i, Lambda2Void<Integer, T1> fn, Byte flags) {
      int high = (int) ((long) i >> 32);
      assert high > 0;
      int low = (int) (i & MASK);
      assert low > 0;
      final T1 ret = getNode(high, flags);
      fn.call(low, ret);
    }
  };

  public int size() {
    return nodes.length;
  }

  public T1 getNode(int i) {
    return getNode(i, MethodFlag.ALL);
  }

  public T1 getNode(int i, byte flags) {
    T1 ret = nodes[i];
    acquireAbstractLock(ret, flags);
    return ret;
  }

  public void setNode(int i, T1 node) {
    setNode(i, node, MethodFlag.ALL);
  }

  public void setNode(int i, T1 node, byte flags) {
    acquireAbstractLock(node, flags);
    nodes[i] = node;
  }

  public void map(LambdaVoid<T1> fn) {
    for (int i = 0; i < nodes.length; i++) {
      T1 node = nodes[i];
      fn.call(node);
    }
  }

  public <T2> void map(Lambda2Void<T1, T2> fn, T2 arg0) {
    for (int i = 0; i < nodes.length; i++) {
      T1 node = nodes[i];
      fn.call(node, arg0);
    }
  }

  //TODO: rest of maps over graph
  public void map(T1 src, LambdaVoid<T1> fn, byte domain) {
    map(src, fn, domain, MethodFlag.ALL);
  }

  public void map(T1 src, LambdaVoid<T1> fn, byte domain, byte flags) {
    acquireAbstractLock(src, flags);
    src.___map(ADAPTER1_1, fn, domain, flags);
  }

  public <T2> void map(T1 src, Lambda2Void<T1, T2> fn, T2 arg2, byte domain) {
    map(src, fn, arg2, domain, MethodFlag.ALL);
  }

  public <T2> void map(T1 src, Lambda2Void<T1, T2> fn, T2 arg2, byte domain, byte flags) {
    acquireAbstractLock(src, flags);
    src.___map(ADAPTER_11, (Lambda2Void<T1, Object>) fn, arg2, domain, flags);
  }

  public void map(T1 src, Lambda2Void<Integer, T1> fn, byte domain, byte flags) {
    acquireAbstractLock(src, flags);
    src.___map(ADAPTER2, fn, domain, flags);
  }

  public boolean addNeighbor(int n1, int n2, byte domain) {
    return addNeighbor(n1, n2, domain, MethodFlag.ALL);
  }

  public boolean addNeighbor(int n1, int n2, byte domain, byte flags) {
    Node node1 = nodes[n1];
    acquireAbstractLock(node1, nodes[2], flags);
    return node1.___addNeighbor(n2, domain);
  }

  public boolean addNeighbor(int n1, int n2, int edgeValue, byte domain) {
    return addNeighbor(n1, n2, edgeValue, domain, MethodFlag.ALL);
  }

  public boolean addNeighbor(int n1, int n2, int edgeValue, byte domain, byte flags) {
    Node node1 = nodes[n1];
    acquireAbstractLock(node1, nodes[2], flags);
    return node1.___addNeighbor(n2, edgeValue, domain);
  }

  public void clear() {
    for (int i = 0; i < nodes.length; i++) {
      nodes[i] = null;
    }
  }

  private static void acquireAbstractLock(GObject ret, byte flags) {
    Iteration.access(ret, flags);
  }

  private static void acquireAbstractLock(GObject l1, GObject l2, byte flags) {
    Iteration.access(l1, flags);
    Iteration.access(l2, flags);
  }
}
