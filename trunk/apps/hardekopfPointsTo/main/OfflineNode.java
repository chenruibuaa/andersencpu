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

 File: OfflineNode.java
 */

package hardekopfPointsTo.main;

import galois.runtime.Iteration;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import util.MutableInteger;
import util.fn.Lambda2Void;
import util.fn.Lambda3Void;
import util.fn.Lambda4Void;
import util.fn.LambdaVoid;
import util.ints.IntSparseBitVector;

class OfflineNode implements Node {
  static int NODE_RANK_MIN;
  private static final int INDIRECT_MASK = 1 << 31;
  private static final int SCC_ROOT_MASK = 1 << 30;
  private static final int FLAGS_MASK = INDIRECT_MASK | SCC_ROOT_MASK;
  static final byte OUT_EDGES = 0;
  static final byte IN_EDGES = 1;
  static final byte IMPLICIT_IN_EDGES = 2;

  private static final AtomicIntegerFieldUpdater<OfflineNode> inDegreeUpdater = AtomicIntegerFieldUpdater.newUpdater(
      OfflineNode.class, "inDegree");

  private int representative;
  int id, dfsId;
  IntSparseBitVector label;
  private final IntSparseBitVector edges, incomingEdges, implicitIncomingEdges;
  int flagsAndMainNode;
  private volatile int inDegree;

  //private final AtomicReference<Iteration> owner;

  OfflineNode(int id, boolean indirect) {
    this.id = id;
    representative = NODE_RANK_MIN;
    label = new IntSparseBitVector();
    edges = new IntSparseBitVector();
    incomingEdges = new IntSparseBitVector();
    implicitIncomingEdges = new IntSparseBitVector();
    // set all the flags and main node at once
    flagsAndMainNode = 0;
    setIndirect(indirect);
    inDegree = -1;
    //owner = new AtomicReference<Iteration>();
  }

  void reset(int id, boolean indirect) {
    this.id = id;
    representative = NODE_RANK_MIN;
    label.clear();
    edges.clear();
    incomingEdges.clear();
    implicitIncomingEdges.clear();
    dfsId = 0;
    // set all the flags and main node at once
    flagsAndMainNode = 0;
    setIndirect(indirect);
    inDegree = -1;
  }

  @Override
  public <T1 extends Node> void ___map(Lambda3Void<Integer, LambdaVoid<T1>, Byte> fn1, LambdaVoid<T1> fn2, byte domain,
      byte flags) {
    if (domain == OUT_EDGES) {
      edges.map(fn1, fn2, flags);
      return;
    } else if (domain == IN_EDGES) {
      incomingEdges.map(fn1, fn2, flags);
      return;
    } else if (domain == IMPLICIT_IN_EDGES) {
      implicitIncomingEdges.map(fn1, fn2, flags);
      return;
    }
    throw new RuntimeException();
  }

  @Override
  public <T1 extends Node, T2> void ___map(Lambda4Void<Integer, Lambda2Void<T1, T2>, T2, Byte> fn1,
      Lambda2Void<T1, T2> fn2, T2 arg2, byte domain, byte flags) {
    if (domain == OUT_EDGES) {
      edges.map(fn1, fn2, arg2, flags);
      return;
    } else if (domain == IN_EDGES) {
      incomingEdges.map(fn1, fn2, arg2, flags);
      return;
    } else if (domain == IMPLICIT_IN_EDGES) {
      implicitIncomingEdges.map(fn1, fn2, arg2, flags);
      return;
    }
    throw new RuntimeException();
  }

  @Override
  public <T1 extends Node> void ___map(Lambda3Void<Long, Lambda2Void<Integer, T1>, Byte> fn1,
      Lambda2Void<Integer, T1> fn2, byte domain, byte flags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean ___addNeighbor(int n, byte domain) {
    if (domain == OUT_EDGES) {
      return edges.concurrentAdd(n);
    } else if (domain == IN_EDGES) {
      return incomingEdges.concurrentAdd(n);
    } else if (domain == IMPLICIT_IN_EDGES) {
      return implicitIncomingEdges.concurrentAdd(n);
    }
    throw new RuntimeException();
  }

  @Override
  public boolean ___addNeighbor(int n, int edgeTag, byte domain) {
    throw new UnsupportedOperationException();
  }

  OfflineNode getRep(final MultiGraph<OfflineNode> offlineGraph, byte flags) {
    OfflineNode curr = this;
    int representative = curr.representative;
    while (representative < NODE_RANK_MIN) {
      curr = offlineGraph.getNode(representative, flags);
      representative = curr.representative;
    }
    return curr;
  }

  boolean isRep() {
    return representative >= NODE_RANK_MIN;
  }

  OfflineNode merge(OfflineNode offlineNode2) {
    assert this.isRep() && offlineNode2.isRep();
    assert this != offlineNode2;
    OfflineNode offlineNode1 = this;
    assert offlineNode1.dfsId > 0 && offlineNode2.dfsId > 0;
    int rank1 = offlineNode1.representative;
    int rank2 = offlineNode2.representative;
    // n1 is the representative
    if (rank1 < rank2) {
      OfflineNode offlineNodeTmp = offlineNode1;
      offlineNode1 = offlineNode2;
      offlineNode2 = offlineNodeTmp;
    } else if (rank1 == rank2) {
      offlineNode1.representative++;
    }
    //System.err.println("    offline merge " + offlineNode1.id  + " <= " +  offlineNode2.id);
    offlineNode2.representative = offlineNode1.id;
    offlineNode1.edges.unionTo(offlineNode2.edges);
    offlineNode1.incomingEdges.unionTo(offlineNode2.incomingEdges);
    offlineNode1.setIndirect(offlineNode1.isIndirect() || offlineNode2.isIndirect());
    // no need to transfer labels (they are empty)
    // no need to transfer implicit edges, since the are not used after the HVN DFS
    offlineNode2.edges.clear();
    offlineNode2.incomingEdges.clear();
    offlineNode2.implicitIncomingEdges.clear();
    return offlineNode1;
  }

  int computeIncomingDegree(final MultiGraph<OfflineNode> offlineGraph, final byte flags) {
    if (incomingEdges.isEmpty()) {
      inDegree = 0;
      return 0;
    }
    final MutableInteger ret = new MutableInteger(0);
    final IntSparseBitVector seen = new IntSparseBitVector();
    incomingEdges.map(new Lambda2Void<Integer, OfflineNode>() {
      @Override
      public void call(Integer next, OfflineNode src) {
        OfflineNode offlineNode = offlineGraph.getNode(next, flags).getRep(offlineGraph, flags);
        assert offlineNode.dfsId > 0;
        if (src != offlineNode && seen.add(offlineNode.dfsId)) {
          ret.add(1);
        }
      }
    }, this);
    int in = ret.get();
    inDegree = in;
    return in;
  }

  int decrementIncomingInDegree() {
    if (inDegree == 1) {
      return 0;
    }
    return inDegreeUpdater.decrementAndGet(this);
  }

  boolean isIndirect() {
    return (flagsAndMainNode & INDIRECT_MASK) != 0;
  }

  void setIndirect(boolean indirect) {
    if (indirect) {
      flagsAndMainNode |= INDIRECT_MASK;
    } else {
      flagsAndMainNode &= (~INDIRECT_MASK);
    }
  }

  boolean isSccRoot() {
    return (flagsAndMainNode & SCC_ROOT_MASK) != 0;
  }

  void setSccRoot(boolean sccRoot) {
    if (sccRoot) {
      flagsAndMainNode |= SCC_ROOT_MASK;
    } else {
      flagsAndMainNode &= (~SCC_ROOT_MASK);
    }
  }

  int getMainNode() {
    return flagsAndMainNode & ~FLAGS_MASK;
  }

  void setMainNode(int main) {
    int currFlags = FLAGS_MASK & flagsAndMainNode;
    flagsAndMainNode = main | currFlags;
  }

  @Override
  public void access(Iteration it, byte flags) {
  }

  @Override
  public int hashCode() {
    return id * 31;
  }
}
