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

File: VectorMorphObjectGraph.java 

 */

package galois.objects.graph;

import galois.objects.GObject;
import galois.objects.MethodFlag;
import galois.runtime.GaloisRuntime;
import galois.runtime.PmapContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import util.MutableInteger;
import util.fn.Lambda2Void;
import util.fn.Lambda3Void;
import util.fn.LambdaVoid;

final class OldVectorMorphObjectGraph<N extends GObject, E> implements ObjectGraph<N, E> {
  private static final int CACHE_MULTIPLE = 16;
  private static final int chunkSize = 16 * GaloisRuntime.getRuntime().getMaxThreads();
  
  private final LinkedNode[] heads;
  private final boolean isDirected;

  OldVectorMorphObjectGraph() {
    this(true);
  }

  OldVectorMorphObjectGraph(boolean isDirected) {
    this.isDirected = isDirected;
    heads = new LinkedNode[GaloisRuntime.getRuntime().getMaxThreads() * CACHE_MULTIPLE];
  }

  private static int getIndex(int tid) {
    return tid * CACHE_MULTIPLE;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private GraphNode downcast(GNode n) {
    return (GraphNode) n;
  }

  @Override
  public GNode<N> createNode(final N n) {
    return createNode(n, MethodFlag.ALL);
  }
  @Override
  public GNode<N> createNode(final N n, Object arg) {
    return createNode(n, MethodFlag.ALL);
  }
  @Override
  public GNode<N> createNode(final N n, Object arg, byte flags) {
    return createNode(n, MethodFlag.ALL);
  }
  @Override
  public GNode<N> createNode(final N n, byte flags) {
    GNode<N> ret = new GraphNode(n, isDirected);
    ObjectGraphLocker.createNodeEpilog(ret, flags);
    return ret;
  }

  @Override
  public boolean add(GNode<N> src) {
    return add(src, MethodFlag.ALL);
  }

  @Override
  public boolean add(GNode<N> src, byte flags) {
    ObjectGraphLocker.addNodeProlog(src, flags);
    GraphNode gsrc = downcast(src);
    if (gsrc.add(this)) {
      ObjectGraphLocker.addNodeEpilog(this, src, flags);
      return true;
    }
    return false;
  }

  @Override
  public boolean remove(GNode<N> src) {
    return remove(src, MethodFlag.ALL);
  }

  @Override
  public boolean remove(GNode<N> src, byte flags) {
    // grab a lock on src if needed
    if (!contains(src, flags)) {
      return false;
    }
    // grab a lock on the neighbors + store undo information if needed
    ObjectGraphLocker.removeNodeProlog(this, src, flags);
    GraphNode gsrc = downcast(src);
    boolean ret = gsrc.remove(this);
    // has to be there, because containsNode returned true and we have the lock
    // on the node
    assert ret;
    return true;
  }

  @Override
  public boolean contains(GNode<N> src) {
    return contains(src, MethodFlag.ALL);
  }

  @Override
  public boolean contains(GNode<N> src, byte flags) {
    ObjectGraphLocker.containsNodeProlog(src, flags);
    GraphNode gsrc = downcast(src);
    return gsrc.inGraph(this);
  }

  @Override
  public int size() {
    return size(MethodFlag.ALL);
  }

  @Override
  public int size(byte flags) {
    ObjectGraphLocker.sizeProlog(flags);
    final MutableInteger retval = new MutableInteger();
    map(new LambdaVoid<GNode<N>>() {
      @Override
      public void call(GNode<N> arg0) {
        retval.add(1);
      }
    }, flags);
    return retval.get();
  }

  @Override
  public boolean addNeighbor(GNode<N> src, GNode<N> dst) {
    throw new UnsupportedOperationException("addNeighbor not supported in EdgeGraphs. Use createEdge/addEdge instead");
  }

  @Override
  public boolean addNeighbor(GNode<N> src, GNode<N> dst, byte flags) {
    throw new UnsupportedOperationException("addNeighbor not supported in EdgeGraphs. Use createEdge/addEdge instead");
  }

  @Override
  public boolean removeNeighbor(GNode<N> src, GNode<N> dst) {
    return removeNeighbor(src, dst, MethodFlag.ALL);
  }

  @Override
  public boolean removeNeighbor(GNode<N> src, GNode<N> dst, byte flags) {
    ObjectGraphLocker.removeNeighborProlog(src, dst, flags);
    GraphNode gsrc = downcast(src);
    GraphNode gdst = downcast(dst);
    int index = gsrc.outNeighbors.indexOf(gdst);
    if (index < 0) {
      return false;
    }
    // E data = gsrc.outData.get(index);
    // src has to be connected to dst
    gsrc.removeNeighborRetEdgeData(gdst, true);
    // dst might no be connected to src if src==dst && the graph is undirected
    gdst.removeNeighbor(gsrc, false);
    // ObjectGraphLocker.removeNeighborEpilog(this, src, dst, data, flags);
    return true;
  }

  @Override
  public boolean hasNeighbor(GNode<N> src, GNode<N> dst) {
    return hasNeighbor(src, dst, MethodFlag.ALL);
  }

  @Override
  public boolean hasNeighbor(GNode<N> src, GNode<N> dst, byte flags) {
    ObjectGraphLocker.hasNeighborProlog(src, dst, flags);
    GraphNode gsrc = downcast(src);
    GraphNode gdst = downcast(dst);
    boolean ret = gsrc.outNeighbors.contains(gdst);
    assert ret == gdst.inNeighbors.contains(gsrc);
    return ret;
  }

  @Override
  public void mapInNeighbors(GNode<N> src, LambdaVoid<GNode<N>> lambda) {
    mapInNeighbors(src, lambda, MethodFlag.ALL);
  }

  @Override
  public void mapInNeighbors(GNode<N> src, LambdaVoid<GNode<N>> lambda, byte flags) {
    ObjectGraphLocker.mapInNeighborsProlog(this, src, flags);
    GraphNode gsrc = downcast(src);
    List<GraphNode> neighbors = gsrc.inNeighbors;
    final int size = neighbors.size();
    for (int i = 0; i < size; i++) {
      GraphNode neighbor = neighbors.get(i);
      lambda.call(neighbor);
    }
  }

  @Override
  public int inNeighborsSize(GNode<N> src) {
    return inNeighborsSize(src, MethodFlag.ALL);
  }

  @Override
  public int inNeighborsSize(GNode<N> src, byte flags) {
    ObjectGraphLocker.inNeighborsSizeProlog(this, src, flags);
    GraphNode gsrc = downcast(src);
    return gsrc.inNeighbors.size();
  }

  @Override
  public int outNeighborsSize(GNode<N> src) {
    return outNeighborsSize(src, MethodFlag.ALL);
  }

  @Override
  public int outNeighborsSize(GNode<N> src, byte flags) {
    ObjectGraphLocker.outNeighborsSizeProlog(src, flags);
    GraphNode gsrc = downcast(src);
    return gsrc.outNeighbors.size();
  }

  @Override
  public boolean addEdge(GNode<N> src, GNode<N> dst, E data) {
    return addEdge(src, dst, data, MethodFlag.ALL);
  }

  @Override
  public boolean addEdge(GNode<N> src, GNode<N> dst, final E data, byte flags) {
    ObjectGraphLocker.addEdgeProlog(src, dst, flags);
    GraphNode gsrc = downcast(src);
    GraphNode gdst = downcast(dst);
    // if the edge is already there, do not allow overwriting of data (use
    // setEdgeData instead)
    if (gsrc.addNeighbor(gdst, data, true)) {
      gdst.addNeighbor(gsrc, data, false);
      ObjectGraphLocker.addEdgeEpilog(this, src, dst, flags);
      return true;
    }
    return false;
  }

  @Override
  public E getEdgeData(GNode<N> src, GNode<N> dst) {
    return getEdgeData(src, dst, MethodFlag.ALL);
  }

  @Override
  public E getEdgeData(GNode<N> src, GNode<N> dst, byte flags) {
    return getEdgeData(src, dst, flags, flags);
  }

  @Override
  public E getEdgeData(GNode<N> src, GNode<N> dst, byte edgeFlags, byte dataFlags) {
    ObjectGraphLocker.getEdgeDataProlog(src, dst, edgeFlags);
    GraphNode gsrc = downcast(src);
    GraphNode gdst = downcast(dst);
    int index = gsrc.outNeighbors.indexOf(gdst);
    if (index >= 0) {
      E ret = gsrc.outData.get(index);
      ObjectGraphLocker.getEdgeDataEpilog(ret, dataFlags);
      return ret;
    }
    return null;
  }

  @Override
  public E setEdgeData(GNode<N> src, GNode<N> dst, E d) {
    return setEdgeData(src, dst, d, MethodFlag.ALL);
  }

  @Override
  public E setEdgeData(GNode<N> src, GNode<N> dst, final E data, byte flags) {
    ObjectGraphLocker.setEdgeDataProlog(src, dst, flags);
    GraphNode gsrc = downcast(src);
    GraphNode gdst = downcast(dst);
    int index = gsrc.outNeighbors.indexOf(gdst);
    if (index < 0) {
      return null;
    }
    E oldData = gsrc.outData.get(index);
    // fast check to avoid redundant work
    if (oldData != data) {
      gsrc.outData.set(index, data);
      if (gsrc != gdst || isDirected) {
        index = gdst.inNeighbors.indexOf(gsrc);
        assert oldData == gdst.inData.get(index);
        gdst.inData.set(index, data);
      }
      ObjectGraphLocker.setEdgeDataEpilog(this, src, dst, oldData, flags);
    }

    return oldData;
  }

  @Override
  public boolean isDirected() {
    return isDirected;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void pmap(LambdaVoid<GNode<N>> body, PmapContext ctx) {
    AtomicInteger cur = (AtomicInteger) ctx.getContextObject();
    
    for (int tid = cur.getAndIncrement(); tid < GaloisRuntime.getRuntime().getMaxThreads(); tid = cur.getAndIncrement()) {
      LinkedNode curr = heads[getIndex(tid)];
      while (curr != null) {
        if (!curr.isDummy()) {
          GraphNode gsrc = (GraphNode) curr;
          assert gsrc.in;
          body.call(gsrc);
        }
        curr = curr.getNext();
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

  @SuppressWarnings("unchecked")
  @Override
  public void map(LambdaVoid<GNode<N>> body, byte flags) {
    ObjectGraphLocker.mapProlog(flags);
    for (int tid = 0; tid < GaloisRuntime.getRuntime().getMaxThreads(); tid++) {
      LinkedNode curr = heads[getIndex(tid)];
      while (curr != null) {
        if (!curr.isDummy()) {
          GraphNode gsrc = (GraphNode) curr;
          assert gsrc.in;
          body.call(gsrc);
        }
        curr = curr.getNext();
      }
    }
  }

  @Override
  public <A1> void map(Lambda2Void<GNode<N>, A1> body, A1 arg1) {
    map(body, arg1, MethodFlag.ALL);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <A1> void map(Lambda2Void<GNode<N>, A1> body, A1 arg1, byte flags) {
    ObjectGraphLocker.mapProlog(flags);
    for (int tid = 0; tid < GaloisRuntime.getRuntime().getMaxThreads(); tid++) {
      LinkedNode curr = heads[getIndex(tid)];
      while (curr != null) {
        if (!curr.isDummy()) {
          GraphNode gsrc = (GraphNode) curr;
          assert gsrc.in;
          body.call(gsrc, arg1);
        }
        curr = curr.getNext();
      }
    }
  }

  @Override
  public <A1, A2> void map(Lambda3Void<GNode<N>, A1, A2> body, A1 arg1, A2 arg2) {
    map(body, arg1, arg2, MethodFlag.ALL);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <A1, A2> void map(Lambda3Void<GNode<N>, A1, A2> body, A1 arg1, A2 arg2, byte flags) {
    ObjectGraphLocker.mapProlog(flags);
    for (int tid = 0; tid < GaloisRuntime.getRuntime().getMaxThreads(); tid++) {
      LinkedNode curr = heads[getIndex(tid)];
      while (curr != null) {
        if (!curr.isDummy()) {
          GraphNode gsrc = (GraphNode) curr;
          assert gsrc.in;
          body.call(gsrc, arg1, arg2);
        }
        curr = curr.getNext();
      }
    }
  }

  @Override
  public Iterator<GNode<N>> iterator() {
    return new GraphIterator();
  }
  
  private static interface LinkedNode {
    public void setNext(LinkedNode next);

    public LinkedNode getNext();

    public boolean isDummy();
  }

  private static class DummyLinkedNode implements LinkedNode {
    private LinkedNode next;

    @Override
    public void setNext(LinkedNode next) {
      this.next = next;
    }

    @Override
    public LinkedNode getNext() {
      return next;
    }

    @Override
    public boolean isDummy() {
      return true;
    }
  }

  private final class GraphNode extends ConcurrentGNode<N> implements LinkedNode {
    private static final int NUM_NEIGHBORS = 4;
    private final List<GraphNode> inNeighbors;
    private final List<E> inData;
    private final List<GraphNode> outNeighbors;
    private final List<E> outData;
    private N data;
    private LinkedNode dummy;
    private LinkedNode next;
    private boolean in;

    private GraphNode(N d, boolean isDirected) {
      outNeighbors = new ArrayList<GraphNode>(NUM_NEIGHBORS);
      outData = new ArrayList<E>(NUM_NEIGHBORS);
      if (isDirected) {
        inNeighbors = new ArrayList<GraphNode>(NUM_NEIGHBORS);
        inData = new ArrayList<E>(NUM_NEIGHBORS);
      } else {
        inNeighbors = outNeighbors;
        inData = outData;
      }
      data = d;
    }

    private boolean inGraph(OldVectorMorphObjectGraph<N, E> g) {
      return OldVectorMorphObjectGraph.this == g && in;
    }

    private boolean add(OldVectorMorphObjectGraph<N, E> g) {
      if (OldVectorMorphObjectGraph.this != g) {
        // XXX(ddn): Nodes could belong to more than 1 graph, but since
        // this rarely happens in practice, simplify implementation
        // assuming that this doesn't occur
        throw new UnsupportedOperationException("cannot add nodes created by a different graph");
      }

      if (!in) {
        in = true;
        dummy = new DummyLinkedNode();
        dummy.setNext(this);

        int tid = GaloisRuntime.getRuntime().getThreadId();
        LinkedNode currHead = heads[getIndex(tid)];
        next = currHead;
        heads[getIndex(tid)] = dummy;
        return true;
      }

      return false;
    }

    private boolean addNeighbor(GraphNode node, E data, boolean fromOutNeighbors) {
      List<GraphNode> neighbors = fromOutNeighbors ? outNeighbors : inNeighbors;
      if (!neighbors.contains(node)) {
        neighbors.add(node);
        List<E> neighborsData = fromOutNeighbors ? outData : inData;
        neighborsData.add(data);
        return true;
      }
      return false;
    }

    private boolean remove(OldVectorMorphObjectGraph<N, E> g) {
      if (!inGraph(g)) {
        return false;
      }
      in = false;
      dummy.setNext(next);
      final int outNeighborsSize = outNeighbors.size();
      for (int i = 0; i < outNeighborsSize; i++) {
        GraphNode gdst = outNeighbors.get(i);
        if (this != gdst || isDirected) {
          gdst.removeNeighbor(this, false);
        }
      }
      if (isDirected) {
        int inNeighborsSize = inNeighbors.size();
        for (int i = 0; i < inNeighborsSize; i++) {
          GraphNode gsrc = inNeighbors.get(i);
          gsrc.removeNeighbor(this, true);
        }
        inNeighbors.clear();
        inData.clear();
      }
      outNeighbors.clear();
      outData.clear();
      return true;
    }

    private boolean removeNeighbor(GraphNode node, boolean fromOutNeighbors) {
      List<GraphNode> neighbors = fromOutNeighbors ? outNeighbors : inNeighbors;
      int index = neighbors.indexOf(node);
      if (index < 0) {
        return false;
      }
      removeAndGetEdgeData(neighbors, index, fromOutNeighbors);
      return true;
    }

    // same as before BUT the neighbor has to be there, so we return the edge
    // data instead
    private E removeNeighborRetEdgeData(GraphNode node, boolean fromOutNeighbors) {
      List<GraphNode> neighbors = fromOutNeighbors ? outNeighbors : inNeighbors;
      assert neighbors.contains(node);
      int index = neighbors.indexOf(node);
      return removeAndGetEdgeData(neighbors, index, fromOutNeighbors);
    }

    private E removeAndGetEdgeData(List<GraphNode> neighbors, int index, boolean fromOutNeighbors) {
      List<E> data = fromOutNeighbors ? outData : inData;
      int indexLast = neighbors.size() - 1;
      if (index < indexLast) {
        // swap the element + data at this index with the one in the last
        // position
        // the swap avoids shifting the contents of the arraylist
        neighbors.set(index, neighbors.remove(indexLast));
        final E ret = data.remove(indexLast);
        data.set(index, ret);
        return ret;
      }
      assert index == indexLast;
      neighbors.remove(indexLast);
      return data.remove(indexLast);
    }

    @Override
    public boolean isDummy() {
      return false;
    }

    @Override
    public LinkedNode getNext() {
      return next;
    }

    @Override
    public void setNext(LinkedNode next) {
      this.next = next;
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
      ObjectGraphLocker.getNodeDataProlog(this, nodeFlags);
      N ret = this.data;
      ObjectGraphLocker.getNodeDataEpilog(ret, dataFlags);
      return ret;
    }

    @Override
    public N setData(N data) {
      return setData(data, MethodFlag.ALL);
    }

    @Override
    public N setData(N data, byte flags) {
      ObjectGraphLocker.setNodeDataProlog(this, flags);
      N oldData = this.data;
      // fast check to avoid redundant calls to the CM
      if (oldData != data) {
        this.data = data;
        ObjectGraphLocker.setNodeDataEpilog(this, oldData, flags);
      }
      return oldData;
    }

    @Override
    public void map(LambdaVoid<GNode<N>> body) {
      map(body, MethodFlag.ALL);
    }

    @Override
    public void pmap(LambdaVoid<GNode<N>> body, PmapContext ctx) {
      int size = outNeighbors.size();
      AtomicInteger cur = (AtomicInteger) ctx.getContextObject();

      for (int i = cur.getAndAdd(chunkSize); i < size; i = cur.getAndAdd(chunkSize)) {
        for (int j = 0; j < chunkSize; j++) {
          if (i + j >= size)
            break;

          GraphNode node = outNeighbors.get(i + j);
          body.call(node);
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
    public void map(LambdaVoid<GNode<N>> body, byte flags) {
      ObjectGraphLocker.mapOutNeighborsProlog(this, flags);
      int size = outNeighbors.size();
      for (int i = 0; i < size; i++) {
        GraphNode node = outNeighbors.get(i);
        body.call(node);
      }
    }

    @Override
    public <A1> void map(Lambda2Void<GNode<N>, A1> body, A1 arg1) {
      map(body, arg1, MethodFlag.ALL);
    }

    @Override
    public <A1> void map(Lambda2Void<GNode<N>, A1> body, A1 arg1, byte flags) {
      ObjectGraphLocker.mapOutNeighborsProlog(this, flags);
      int size = outNeighbors.size();
      for (int i = 0; i < size; i++) {
        GraphNode node = outNeighbors.get(i);
        body.call(node, arg1);
      }
    }

    @Override
    public <A1, A2> void map(Lambda3Void<GNode<N>, A1, A2> body, A1 arg1, A2 arg2) {
      map(body, arg1, arg2, MethodFlag.ALL);
    }

    @Override
    public <A1, A2> void map(Lambda3Void<GNode<N>, A1, A2> body, A1 arg1, A2 arg2, byte flags) {
      ObjectGraphLocker.mapOutNeighborsProlog(this, flags);
      int size = outNeighbors.size();
      for (int i = 0; i < size; i++) {
        GraphNode node = outNeighbors.get(i);
        body.call(node, arg1, arg2);
      }
    }
  }
  
  private class GraphIterator implements Iterator<GNode<N>> {
    private GNode<N> next;
    private LinkedNode curr;
    private int tid;
    
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
    
    @SuppressWarnings("unchecked")
    private void advance() {
      while (tid < GaloisRuntime.getRuntime().getMaxThreads()) {
        if (curr == null) {
          curr = heads[getIndex(tid)];
        }
        
        while (curr != null) {
          if (!curr.isDummy()) {
            GraphNode gsrc = (GraphNode) curr;
            assert gsrc.in;
            next = gsrc;
            curr = curr.getNext();
            return;
          }
          curr = curr.getNext();
        }
        
        tid++;
        curr = null;
      }

      next = null;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
