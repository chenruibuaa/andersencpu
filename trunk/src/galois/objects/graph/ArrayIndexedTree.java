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

File: ArrayIndexedTree.java 

 */

package galois.objects.graph;

import galois.objects.GObject;
import galois.objects.MethodFlag;
import galois.runtime.GaloisRuntime;
import galois.runtime.PmapContext;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicStampedReference;

import util.MutableInteger;
import util.fn.Lambda2Void;
import util.fn.Lambda3Void;
import util.fn.LambdaVoid;

/**
 * Implementation of the {@link galois.objects.graph.IndexedGraph} interface.
 * 
 * @param <N>
 *          type of the data contained in a node
 */
public final class ArrayIndexedTree<N extends GObject> implements IndexedGraph<N> {
  private static final int CACHE_MULTIPLE = 16;
  private static final int chunkSize = 16 * GaloisRuntime.getRuntime().getMaxThreads();
  private final int maxNeighbors;
  private final LinkedNode[] heads;

  private ArrayIndexedTree(int capacity) {
    assert (capacity > 0);
    maxNeighbors = capacity;
    heads = new LinkedNode[GaloisRuntime.getRuntime().getMaxThreads() * CACHE_MULTIPLE];
  }

  /**
   * A {@link ArrayIndexedTree} builder, providing combinations of several
   * features.
   */
  public static class Builder {
    private int branchingFactor = 2;
    private boolean serial = false;

    /**
     * Constructs a new builder instance with the following default settings:
     * the tree will be parallel, and have a branching factor of two
     */
    public Builder() {
    }

    /**
     * Specifies the maximum number of children for a node in the tree.
     * 
     * @param branchingFactor
     *          Branching factor of the tree
     */
    public Builder branchingFactor(int branchingFactor) {
      this.branchingFactor = branchingFactor;
      return this;
    }

    /**
     * Indicates whether the implementation of the tree about to be created is
     * serial (there is no concurrency or transactional support) or parallel
     * (can be safely used within Galois iterators). For example, a tree that is
     * purely thread local can benefit from using a serial implementation, which
     * is expected to add no overheads due to concurrency or the runtime system.
     * 
     * @param serial
     *          boolean value that indicates whether the tree is serial or not.
     */
    public Builder serial(boolean serial) {
      this.serial = serial;
      return this;
    }

    /**
     * Builds the final tree. This method does not alter the state of this
     * builder instance, so it can be invoked again to create multiple
     * independent trees.
     * 
     * @param <N>
     *          the type of the object stored in each node
     * @return a indexed tree with the requested features
     */
    public <N extends GObject> IndexedGraph<N> create() {
      IndexedGraph<N> retval;
      if (serial || GaloisRuntime.getRuntime().useSerial()) {
        retval = new SerialArrayIndexedTree<N>(branchingFactor);
      } else {
        retval = new ArrayIndexedTree<N>(branchingFactor);
      }

      return retval;
    }
  }

  private static int getIndex(int tid) {
    return tid * CACHE_MULTIPLE;
  }

  @Override
  public final GNode<N> createNode(N n) {
    return createNode(n, MethodFlag.ALL);
  }

  
  @Override
  public GNode<N> createNode(N n, byte flags) {
    GNode<N> ret = new GraphNode(n);
    ObjectGraphLocker.createNodeEpilog(ret, flags);
    return ret;
  }

  @Override
  public GNode<N> createNode(N n, Object arg) {
    return createNode(n, MethodFlag.ALL);
  }
  
  @Override
  public GNode<N> createNode(N n, Object arg, byte flags) {
    return createNode(n, flags);
  }
  
  @Override
  public final boolean add(GNode<N> src) {
    return add(src, MethodFlag.ALL);
  }

  @Override
  public boolean add(GNode<N> src, byte flags) {
    IndexedTreeLocker.addNodeProlog(src, flags);
    if (((GraphNode) src).add(this)) {
      IndexedTreeLocker.addNodeEpilog(this, src, flags);
      return true;
    }
    return false;
  }

  @Override
  public final boolean remove(GNode<N> src) {
    return remove(src, MethodFlag.ALL);
  }

  @Override
  public boolean remove(GNode<N> src, byte flags) {
    // grab a lock on src if needed
    if (!contains(src, flags)) {
      return false;
    }
    IndexedTreeLocker.removeNodeProlog(src, flags);
    GraphNode ignsrc = (GraphNode) src;
    for (int i = 0; i < maxNeighbors; i++) {
      if (ignsrc.child[i] != null) {
        removeNeighbor(ignsrc, i, flags);
      }
    }
    if (ignsrc.parent != null) {
      removeNeighbor(ignsrc.parent, ignsrc, flags);
    }
    boolean ret = ignsrc.remove(this);
    // has to be there, because containsNode returned true and we have the lock
    // on the node
    assert ret;
    return true;
  }

  @Override
  public final boolean contains(GNode<N> src) {
    return contains(src, MethodFlag.ALL);
  }

  @Override
  public boolean contains(GNode<N> src, byte flags) {
    IndexedTreeLocker.containsNodeProlog(src, flags);
    return ((GraphNode) src).inGraph(this);
  }

  @Override
  public final int size() {
    return size(MethodFlag.ALL);
  }

  @Override
  public int size(byte flags) {
    IndexedTreeLocker.sizeProlog(flags);
    final MutableInteger retval = new MutableInteger();
    map(new LambdaVoid<GNode<N>>() {
      @Override
      public void call(GNode<N> arg0) {
        retval.add(1);
      }
    }, flags);
    return retval.get();
  }

  // This method is not supported in an IndexedGraph because it is
  // not clear which neighbor the added neighbor should become.

  @Override
  public final boolean addNeighbor(GNode<N> src, GNode<N> dst) {
    throw new UnsupportedOperationException("addNeighbor not supported in IndexedGraphs");
  }

  @Override
  public boolean addNeighbor(GNode<N> src, GNode<N> dst, byte flags) {
    throw new UnsupportedOperationException("addNeighbor not supported in IndexedGraphs");
  }

  @Override
  public final boolean removeNeighbor(GNode<N> src, GNode<N> dst) {
    return removeNeighbor(src, dst, MethodFlag.ALL);
  }

  @Override
  public boolean removeNeighbor(GNode<N> src, GNode<N> dst, byte flags) {
    IndexedTreeLocker.removeNeighborProlog(src, dst, flags);
    int idx = childIndex(src, dst);
    if (0 <= idx) {
      removeNeighbor(src, idx, flags);
      return true;
    }
    return false;
  }

  @Override
  public final boolean hasNeighbor(GNode<N> src, GNode<N> dst) {
    return hasNeighbor(src, dst, MethodFlag.ALL);
  }

  @Override
  public boolean hasNeighbor(GNode<N> src, GNode<N> dst, byte flags) {
    IndexedTreeLocker.hasNeighborProlog(src, dst, flags);
    return 0 <= childIndex(src, dst);
  }

  @Override
  public final void mapInNeighbors(GNode<N> src, LambdaVoid<GNode<N>> body) {
    mapInNeighbors(src, body, MethodFlag.ALL);
  }

  @Override
  public void mapInNeighbors(GNode<N> src, LambdaVoid<GNode<N>> closure, byte flags) {
    IndexedTreeLocker.mapInNeighborsProlog(this, src, flags);
    GraphNode n = (GraphNode) src;
    if (n.parent != null) {
      closure.call(n.parent);
    }
  }

  @Override
  public final int inNeighborsSize(GNode<N> src) {
    return inNeighborsSize(src, MethodFlag.ALL);
  }

  @Override
  public int inNeighborsSize(GNode<N> src, byte flags) {
    IndexedTreeLocker.inNeighborsSizeProlog(this, src, flags);
    GraphNode ignsrc = (GraphNode) src;
    if (ignsrc.parent != null) {
      return 1;
    }
    return 0;
  }

  @Override
  public final int outNeighborsSize(GNode<N> src) {
    return outNeighborsSize(src, MethodFlag.ALL);
  }

  @Override
  public int outNeighborsSize(GNode<N> src, byte flags) {
    IndexedTreeLocker.outNeighborsSizeProlog(src, flags);
    GraphNode ignsrc = (GraphNode) src;
    int cnt = 0;
    for (int i = 0; i < maxNeighbors; i++) {
      if (ignsrc.child[i] != null) {
        cnt++;
      }
    }
    return cnt;
  }

  @Override
  public boolean isDirected() {
    return true;
  }

  @Override
  public final void setNeighbor(GNode<N> src, GNode<N> dst, int idx) {
    setNeighbor(src, dst, idx, MethodFlag.ALL);
  }

  @Override
  public void setNeighbor(GNode<N> src, GNode<N> dst, int idx, byte flags) {
    IndexedTreeLocker.setNeighborProlog(src, dst, flags);
    GraphNode ignsrc = (GraphNode) src;
    GraphNode old = ignsrc.child[idx];
    if (old != dst) {
      GraphNode igndst = (GraphNode) dst;
      if (igndst.parent != null) {
        removeNeighbor(igndst.parent, igndst, flags);
      }
      igndst.parent = ignsrc;
      if (old != null) {
        removeNeighbor(ignsrc, idx, flags);
      }
      ignsrc.child[idx] = igndst;
      IndexedTreeLocker.setNeighborEpilog(old, flags);
    }
  }

  @Override
  public final GNode<N> getNeighbor(GNode<N> node, int idx) {
    return getNeighbor(node, idx, MethodFlag.ALL);
  }

  @Override
  public GNode<N> getNeighbor(GNode<N> node, int idx, byte flags) {
    IndexedTreeLocker.getNeighborProlog(node, flags);
    GraphNode ignode = (GraphNode) node;
    GNode<N> ret = ignode.child[idx];
    if (ret != null) {
      IndexedTreeLocker.getNeighborEpilog(ret, flags);
    }
    return ret;
  }

  @Override
  public final boolean removeNeighbor(GNode<N> node, int idx) {
    return removeNeighbor(node, idx, MethodFlag.ALL);
  }

  @Override
  public boolean removeNeighbor(GNode<N> src, int idx, byte flags) {
    IndexedTreeLocker.removeNeighborProlog(src, flags);
    GraphNode ignsrc = (GraphNode) src;
    GraphNode child = ignsrc.child[idx];
    if (child != null) {
      ignsrc.child[idx].parent = null;
      ignsrc.child[idx] = null;
      IndexedTreeLocker.removeNeighborEpilog(this, src, child, idx, flags);
      return true;
    }
    return false;
  }

  private int childIndex(GNode<N> src, GNode<N> dst) {
    GraphNode ignsrc = (GraphNode) src;
    List<GraphNode> neighborList = Arrays.asList(ignsrc.child);
    return neighborList.indexOf(dst);
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
    IndexedTreeLocker.mapProlog(flags);

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
    IndexedTreeLocker.mapProlog(flags);

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
    IndexedTreeLocker.mapProlog(flags);

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

  protected final class GraphNode extends ConcurrentGNode<N> implements LinkedNode {
    private final AtomicStampedReference<GraphNode> iterateNext = new AtomicStampedReference<GraphNode>(
        null, 0);
    private final AtomicInteger iterateVersion = new AtomicInteger();
    protected N data;
    protected GraphNode[] child;
    protected GraphNode parent;
    private boolean in;
    private LinkedNode dummy;
    private LinkedNode next;

    @SuppressWarnings("unchecked")
    public GraphNode(N nodedata) {
      data = nodedata;
      child = (GraphNode[]) Array.newInstance(this.getClass(), maxNeighbors);
      Arrays.fill(child, null);
      parent = null;
    }

    private boolean inGraph(ArrayIndexedTree<N> g) {
      return ArrayIndexedTree.this == g && in;
    }

    private boolean add(ArrayIndexedTree<N> g) {
      if (ArrayIndexedTree.this != g) {
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

    private boolean remove(ArrayIndexedTree<N> g) {
      if (inGraph(g)) {
        in = false;
        iterateNext.set(null, 0);
        iterateVersion.set(0);
        dummy.setNext(next);
        return true;
      }
      return false;
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
    public final N getData() {
      return getData(MethodFlag.ALL);
    }

    @Override
    public final N getData(byte flags) {
      return getData(flags, flags);
    }

    @Override
    public N getData(byte nodeFlags, byte dataFlags) {
      IndexedTreeLocker.getNodeDataProlog(this, nodeFlags);
      N ret = data;
      IndexedTreeLocker.getNodeDataEpilog(ret, dataFlags);
      return ret;
    }

    @Override
    public final N setData(N data) {
      return setData(data, MethodFlag.ALL);
    }

    @Override
    public N setData(N data, byte flags) {
      IndexedTreeLocker.setNodeDataProlog(this, flags);
      N oldData = this.data;
      this.data = data;
      IndexedTreeLocker.setNodeDataEpilog(this, oldData, flags);
      return oldData;
    }

    @Override
    public void pmap(LambdaVoid<GNode<N>> body, PmapContext ctx) {
      AtomicInteger cur = (AtomicInteger) ctx.getContextObject();

      for (int i = cur.getAndAdd(chunkSize); i < maxNeighbors; i = cur.getAndAdd(chunkSize)) {
        for (int j = 0; j < chunkSize; j++) {
          if (i + j >= maxNeighbors)
            break;

          GraphNode node = child[i];
          if (node != null) {
            body.call(node);
          }
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
    public final void map(LambdaVoid<GNode<N>> body) {
      map(body, MethodFlag.ALL);
    }

    @Override
    public void map(LambdaVoid<GNode<N>> body, byte flags) {
      IndexedTreeLocker.mapOutNeighborsProlog(this, flags);
      for (int i = 0; i < maxNeighbors; i++) {
        GraphNode c = child[i];
        if (c != null) {
          body.call(c);
        }
      }
    }

    @Override
    public <A1> void map(Lambda2Void<GNode<N>, A1> body, A1 arg1) {
      map(body, arg1, MethodFlag.ALL);
    }

    @Override
    public <A1> void map(Lambda2Void<GNode<N>, A1> body, A1 arg1, byte flags) {
      IndexedTreeLocker.mapOutNeighborsProlog(this, flags);
      for (int i = 0; i < maxNeighbors; i++) {
        GraphNode c = child[i];
        if (c != null) {
          body.call(c, arg1);
        }
      }
    }

    @Override
    public <A1, A2> void map(Lambda3Void<GNode<N>, A1, A2> body, A1 arg1, A2 arg2) {
      map(body, arg1, arg2, MethodFlag.ALL);
    }

    @Override
    public <A1, A2> void map(Lambda3Void<GNode<N>, A1, A2> body, A1 arg1, A2 arg2, byte flags) {
      IndexedTreeLocker.mapOutNeighborsProlog(this, flags);
      for (int i = 0; i < maxNeighbors; i++) {
        GraphNode c = child[i];
        if (c != null) {
          body.call(c, arg1, arg2);
        }
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
