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

File: NoRemovingGraph.java 

 */

package galois.objects.graph;

import galois.objects.GObject;
import galois.objects.MethodFlag;
import galois.runtime.GaloisRuntime;
import galois.runtime.Iteration;
import galois.runtime.PmapContext;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import util.fn.Lambda0Void;
import util.fn.Lambda2Void;
import util.fn.Lambda3Void;
import util.fn.LambdaVoid;

class RefinementIntGraph<N extends GObject> implements IntGraph<N>, BuilderGraph<N,Integer> {
  private static final int chunkSize = 16 * GaloisRuntime.getRuntime().getMaxThreads();
  private final Node[] nodes;
  // XXX(ddn): Potential concurrent bottleneck
  private final AtomicInteger numNodes;

  @SuppressWarnings("unchecked")
  public RefinementIntGraph(int numNodes) {
    nodes = new RefinementIntGraph.Node[numNodes];
    this.numNodes = new AtomicInteger();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private int getId(GNode n) {
    return ((Node) n).id;
  }

  @Override
  public GNode<N> createNode(N n) {
    throw new UnsupportedOperationException();
  }

  @Override
  public GNode<N> createNode(N n, Object degree) {
    return createNode(n, degree, MethodFlag.ALL);
  }

  @Override
  public GNode<N> createNode(N n, byte flags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public GNode<N> createNode(N n, Object degree, byte flags) {
    Node node = new Node(numNodes.getAndIncrement(), n, (Integer) degree);
    return node;
  }

  @Override
  public boolean add(GNode<N> src) {
    return add(src, MethodFlag.ALL);
  }

  @Override
  public boolean add(GNode<N> src, byte flags) {
    Node srcNode = (Node) src;
    nodes[getId(src)] = srcNode;
    return true;
  }

  @Override
  public boolean remove(GNode<N> src) {
    return remove(src, MethodFlag.ALL);
  }

  @Override
  public boolean remove(GNode<N> src, byte flags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean contains(GNode<N> src) {
    return contains(src, MethodFlag.ALL);
  }

  @Override
  public boolean contains(GNode<N> src, byte flags) {
    int idx = getId(src);
    return 0 <= idx && idx < numNodes.get();
  }

  private void acquireAll(byte flags) {
    if (GaloisRuntime.needMethodFlag(flags, MethodFlag.CHECK_CONFLICT)) {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public int size() {
    return size(MethodFlag.ALL);
  }

  @Override
  public int size(byte flags) {
    return numNodes.get();
  }

  @Override
  public boolean addNeighbor(GNode<N> src, GNode<N> dst) {
    return addNeighbor(src, dst, MethodFlag.ALL);
  }

  @Override
  public boolean addNeighbor(GNode<N> src, GNode<N> dst, byte flags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeNeighbor(GNode<N> src, GNode<N> dst) {
    return removeNeighbor(src, dst, MethodFlag.ALL);
  }

  @Override
  public boolean removeNeighbor(GNode<N> src, GNode<N> dst, byte flags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasNeighbor(GNode<N> src, GNode<N> dst) {
    return hasNeighbor(src, dst, MethodFlag.ALL);
  }

  @Override
  public boolean hasNeighbor(GNode<N> src, GNode<N> dst, byte flags) {
    Node srcNode = (Node) src;
    Node dstNode = (Node) dst;
    for (int i = 0; i < srcNode.edgeSize; i++) {
      if (srcNode.edgeIdx[i] == dstNode.id)
        return true;
    }
    return false;
  }

  @Override
  public void mapInNeighbors(GNode<N> src, LambdaVoid<GNode<N>> body) {
    mapInNeighbors(src, body, MethodFlag.ALL);
  }

  /**
   * Does not fail if the set of in neighbors of src is modified within the
   * closure.
   */
  @Override
  public void mapInNeighbors(GNode<N> src, LambdaVoid<GNode<N>> body, byte flags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int inNeighborsSize(GNode<N> src) {
    return inNeighborsSize(src, MethodFlag.ALL);
  }

  @Override
  public int inNeighborsSize(GNode<N> src, byte flags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int outNeighborsSize(GNode<N> src) {
    return outNeighborsSize(src, MethodFlag.ALL);
  }

  @Override
  public int outNeighborsSize(GNode<N> src, byte flags) {
    return ((Node) src).edgeSize;
  }

  @Override
  public boolean addEdge(GNode<N> src, GNode<N> dst, int data) {
    return addEdge(src, dst, data, MethodFlag.ALL);
  }

  @Override
  public boolean addEdge(GNode<N> src, GNode<N> dst, int data, byte flags) {
    Iteration it = Iteration.access(src, flags);
    Iteration.access(it, dst, flags);
    Node srcNode = (Node) src;
    srcNode.edgeIdx[srcNode.edgeSize] = getId(dst);
    srcNode.edgeIdx[srcNode.edgeSize + srcNode.degree()] = data;
    srcNode.edgeSize++;
    return true;
  }

  @Override
  public int getIntEdgeData(GNode<N> src, GNode<N> dst) {
    return getIntEdgeData(src, dst, MethodFlag.ALL);
  }

  private int getEdgeIdx(Node src, Node dst) {
    Node srcNode = (Node) src;
    Node dstNode = (Node) dst;
    for (int i = 0; i < srcNode.edgeSize; i++) {
      if (srcNode.edgeIdx[i] == dstNode.id) {
        return i;
      }
    }
    throw new Error("No such edge");
  }

  @Override
  public int getIntEdgeData(GNode<N> src, GNode<N> dst, byte flags) {
    Iteration it = Iteration.access(src, flags);
    Iteration.access(it, dst, flags);
    Node srcNode = ((Node) src);
    Node dstNode = ((Node) dst);
    if (srcNode.edgeIdx[srcNode.last] == dstNode.id)
      return srcNode.edgeIdx[srcNode.last + srcNode.degree()];
    return srcNode.edgeIdx[getEdgeIdx(srcNode, dstNode) + srcNode.degree()];
  }

  @Override
  public int setIntEdgeData(GNode<N> src, GNode<N> dst, int d) {
    return setIntEdgeData(src, dst, d, MethodFlag.ALL);
  }

  @Override
  public int setIntEdgeData(final GNode<N> src, final GNode<N> dst, int data, byte flags) {
    Iteration it = Iteration.access(src, flags);
    Iteration.access(it, dst, flags);

    Node srcNode = ((Node) src);
    Node dstNode = ((Node) dst);

    if (srcNode.edgeIdx[srcNode.last] == dstNode.id) {
      int index = srcNode.last + srcNode.degree();
      int old = srcNode.edgeIdx[index];
      srcNode.edgeIdx[index] = data;
      return old;
    }
    int index = getEdgeIdx(srcNode, dstNode);
    int old = srcNode.edgeIdx[index];
    srcNode.edgeIdx[index + srcNode.degree()] = data;
    return old;
  }

  @Override
  public boolean isDirected() {
    return true;
  }

  @Override
  public void pmap(LambdaVoid<GNode<N>> body, PmapContext ctx) {
    AtomicInteger cur = (AtomicInteger) ctx.getContextObject();
    int size = numNodes.get();
    for (int i = cur.getAndAdd(chunkSize); i < size; i = cur.getAndAdd(chunkSize)) {
      for (int j = 0; j < chunkSize; j++) {
        int index = i + j;
        if (index >= size) {
          break;
        }

        Node item = nodes[index];
        body.call(item);
      }
    }
  }

  @Override
  public void beforePmap(PmapContext ctx) {
    ctx.setContextObject(new AtomicInteger());
  }
  
  @Override
  public void afterPmap(PmapContext ctx) {
  }
  
  @Override
  public void map(LambdaVoid<GNode<N>> body) {
    map(body, MethodFlag.ALL);
  }

  @Override
  public void map(LambdaVoid<GNode<N>> body, byte flags) {
    // acquireAll(flags);
    for (int i = 0; i < numNodes.get(); i++) {
      body.call(nodes[i]);
    }
  }

  @Override
  public <A1> void map(Lambda2Void<GNode<N>, A1> body, A1 arg1) {
    map(body, arg1, MethodFlag.ALL);
  }

  @Override
  public <A1> void map(Lambda2Void<GNode<N>, A1> body, A1 arg1, byte flags) {
    acquireAll(flags);
    for (int i = 0; i < numNodes.get(); i++) {
      body.call(nodes[i], arg1);
    }
  }

  @Override
  public <A1, A2> void map(Lambda3Void<GNode<N>, A1, A2> body, A1 arg1, A2 arg2) {
    map(body, arg1, arg2, MethodFlag.ALL);
  }

  @Override
  public <A1, A2> void map(Lambda3Void<GNode<N>, A1, A2> body, A1 arg1, A2 arg2, byte flags) {
    acquireAll(flags);
    for (int i = 0; i < numNodes.get(); i++) {
      body.call(nodes[i], arg1, arg2);
    }
  }
  
  @Override
  public Iterator<GNode<N>> iterator() {
    return new GraphIterator();
  }

  @Override
  public BuilderGraphData<N, Integer> getBuilderGraphData() {
    return new MyBuilderGraphData();
  }
  
  private final class Node extends ConcurrentGNode<N> {
    private final int id;
    private N data;
    private int[] edgeIdx; // and data[i] at edgeIndex[i + degree]
    private int edgeSize;
    private int last;

    private Node(int id, N data, int degree) {
      this.id = id;
      this.data = data;
      edgeIdx = new int[degree + degree];
      edgeSize = 0;
    }

    @Override
    public N getData() {
      return getData(MethodFlag.ALL);
    }

    @Override
    public N getData(byte flags) {
      return getData(flags, flags);
    }

    @Override
    public N getData(byte nodeFlags, byte dataFlags) {
      Iteration.access(this, nodeFlags);
      return data;
    }

    @Override
    public N setData(N data) {
      return setData(data, MethodFlag.ALL);
    }

    @Override
    public N setData(N data, byte flags) {
      Iteration.access(this, flags);
      final N oldData = this.data;

      if (oldData != data) {
        this.data = data;

        if (GaloisRuntime.needMethodFlag(flags, MethodFlag.SAVE_UNDO)) {
          GaloisRuntime.getRuntime().onUndo(Iteration.getCurrentIteration(), new Lambda0Void() {
            @Override
            public void call() {
              setData(oldData, MethodFlag.NONE);
            }
          });
        }
      }

      return oldData;
    }

    @Override
    public void pmap(LambdaVoid<GNode<N>> body, PmapContext ctx) {
      AtomicInteger cur = (AtomicInteger) ctx.getContextObject();

      for (int i = cur.getAndAdd(chunkSize); i < edgeSize; i = cur.getAndAdd(chunkSize)) {
        for (int j = 0; j < chunkSize; j++) {
          if (i + j >= edgeSize)
            break;
          int oid = edgeIdx[i + j];
          GNode<N> other = nodes[oid];
          
          body.call(other);
        }
      }
    }

    int degree() {
      return edgeIdx.length / 2;
    }
    
    @Override
    public void beforePmap(PmapContext ctx) {
      ctx.setContextObject(new AtomicInteger());
    }

    @Override
    public void afterPmap(PmapContext ctx) {
    }

    private void acquireNeighbors(byte flags) {
      Iteration it = Iteration.access(this, flags);
      if (it != null) {
        for (int i = 0; i < edgeSize; i++) {
          GNode<N> other = nodes[edgeIdx[i]];
          Iteration.access(it, other, flags);
        }
      }
    }

    @Override
    public void map(LambdaVoid<GNode<N>> body) {
      map(body, MethodFlag.ALL);
    }

    @Override
    public void map(LambdaVoid<GNode<N>> body, byte flags) {
      acquireNeighbors(flags);
      for (int i = 0; i < edgeSize; i++) {
        GNode<N> other = nodes[edgeIdx[i]];
        last = i;
        body.call(other);
      }
    }

    @Override
    public <A1> void map(Lambda2Void<GNode<N>, A1> body, A1 arg1) {
      map(body, arg1, MethodFlag.ALL);
    }

    @Override
    public <A1> void map(Lambda2Void<GNode<N>, A1> body, A1 arg1, byte flags) {
      acquireNeighbors(flags);
      for (int i = 0; i < edgeSize; i++) {
        GNode<N> other = nodes[edgeIdx[i]];
        last = i;
        body.call(other, arg1);
      }
    }

    @Override
    public <A1, A2> void map(Lambda3Void<GNode<N>, A1, A2> body, A1 arg1, A2 arg2) {
      map(body, arg1, arg2, MethodFlag.ALL);
    }

    @Override
    public <A1, A2> void map(Lambda3Void<GNode<N>, A1, A2> body, A1 arg1, A2 arg2, byte flags) {
      acquireNeighbors(flags);
      for (int i = 0; i < edgeSize; i++) {
        GNode<N> other = nodes[edgeIdx[i]];
        last = i;
        body.call(other, arg1, arg2);
      }
    }
  }
  
  private class GraphIterator implements Iterator<GNode<N>> {
    private GNode<N> next;
    private int cur;
    
    public GraphIterator() {
      advance();
    }
    
    @Override
    public boolean hasNext() {
      return next != null;
    }

    @Override
    public GNode<N> next() {
      GNode<N> retval = next;
      advance();
      return retval;
    }
    
    private void advance() {
      while (cur < numNodes.get()){
        next = nodes[cur];
        cur++;
        return;
      }

      next = null;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
  

  private class MyBuilderGraphData extends AbstractEdgeData<Integer> implements BuilderGraphData<N, Integer>, GraphData {
    private final int[] outIdxs;
    private int curPos;

    public MyBuilderGraphData() {
      outIdxs = new int[nodes.length];
      for (int idx = 0; idx < nodes.length; idx++) {
        Node node = nodes[idx];
        outIdxs[idx] = idx > 0 ? outIdxs[idx - 1] + node.edgeSize : node.edgeSize;
      }
    }

    private int checkInt(long x) {
      assert x <= Integer.MAX_VALUE;
      assert x >= 0;
      return (int) x;
    }

    @Override
    public int getNumNodes() {
      return outIdxs.length;
    }

    @Override
    public long getNumEdges() {
      return outIdxs[outIdxs.length - 1];
    }

    @Override
    public long startIn(int id) {
      return startIn(id, true);
    }

    private long startIn(int id, boolean updateCurPos) {
      return 0;
    }

    @Override
    public long endIn(int id) {
      return 0;
    }

    @Override
    public long startOut(int id) {
      return startOut(id, true);
    }

    private long startOut(int id, boolean updateCurPos) {
      int retval = id == 0 ? 0 : outIdxs[id - 1];
      if (updateCurPos)
        curPos = retval;
      return retval;
    }

    @Override
    public long endOut(int id) {
      return outIdxs[id];
    }

    @Override
    public int getInNode(long idx) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getOutNode(long idx) {
      int iidx = checkInt(idx);
      long start = startOut(curPos, false);
      long end = endOut(curPos);
      if (start <= idx && idx < end) {
        ;
      } else {
        int pos = Arrays.binarySearch(outIdxs, iidx);
        if (pos < 0)
          pos = -(pos + 1);
        start = startOut(pos, true);
      }
      Node node = nodes[curPos];
      return node.edgeIdx[iidx - (int) start];
    }

    @Override
    public void setInt(long idx, int v) {
      int iidx = checkInt(idx);
      long start = startOut(curPos, false);
      long end = endOut(curPos);
      if (start <= idx && idx < end) {
        ;
      } else {
        int pos = Arrays.binarySearch(outIdxs, iidx);
        if (pos < 0)
          pos = -(pos + 1);
        start = startOut(pos, true);
      }
      Node node = nodes[curPos];
      node.edgeIdx[iidx - (int) start + node.degree()] = v;
    }

    @Override
    public int getInt(long idx) {
      int iidx = checkInt(idx);
      long start = startOut(curPos, false);
      long end = endOut(curPos);
      if (start <= idx && idx < end) {
        ;
      } else {
        int pos = Arrays.binarySearch(outIdxs, iidx);
        if (pos < 0)
          pos = -(pos + 1);
        start = startOut(pos, true);
      }
      Node node = nodes[curPos];
      return node.edgeIdx[iidx - (int) start + node.degree()];
    }

    @Override
    public Object get(long idx) {
      return getInt(idx);
    }

    @Override
    public void set(long idx, Object v) {
      setInt(idx, (Integer) v);
    }

    @Override
    public GraphData getGraphData() {
      return this;
    }

    @Override
    public NodeData<N> getNodeData() {
      return new NodeData<N>() {
        @Override
        public void set(int id, N v) {
          nodes[id].data = v;
        }

        @Override
        public N get(int id) {
          return nodes[id].data;
        }
      };
    }

    @Override
    public EdgeData<Integer> getEdgeData() {
      return this;
    }
  }
}
