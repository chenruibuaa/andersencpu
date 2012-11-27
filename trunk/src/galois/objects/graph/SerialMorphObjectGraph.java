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

final class SerialMorphObjectGraph<N extends GObject, E> implements ObjectGraph<N, E> {
  private static final int NUM_NEIGHBORS = 4;
  private static final int CACHE_MULTIPLE = 16;
  private static final int chunkSize = 16 * GaloisRuntime.getRuntime().getMaxThreads();

  private final LinkedNode[] heads;
  private final boolean undirected;

  SerialMorphObjectGraph(boolean undirected) {
    this.undirected = undirected;
    heads = new LinkedNode[GaloisRuntime.getRuntime().getMaxThreads() * CACHE_MULTIPLE];
  }

  private static int getIndex(int tid) {
    return tid * CACHE_MULTIPLE;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private Node<N,E> downcast(GNode n) {
    return (Node<N,E>) n;
  }

  @Override
  public GNode<N> createNode(final N n) {
    return createNode(n, MethodFlag.ALL);
  }

  @Override
  public GNode<N> createNode(final N n, byte flags) {
    return createNode(n, NUM_NEIGHBORS, flags);
  }

  @Override
  public GNode<N> createNode(N n, Object arg) {
    return createNode(n, arg, MethodFlag.ALL);
  }

  @Override
  public GNode<N> createNode(N n, Object arg, byte flags) {
    GNode<N> ret = undirected ? new UNode<N,E>(n, (Integer) arg) : new Node<N,E>(n, (Integer) arg);
    return ret;
  }

  @Override
  public boolean add(GNode<N> src) {
    return add(src, MethodFlag.ALL);
  }

  @Override
  public boolean add(GNode<N> src, byte flags) {
    Node<N,E> gsrc = downcast(src);
    int index = getIndex(GaloisRuntime.getRuntime().getThreadId());
    LinkedNode newHead = gsrc.add(heads[index]);
    if (newHead != null) {
      heads[index] = newHead;
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
    Node<N,E> gsrc = downcast(src);
    return gsrc.remove(this);
  }

  @Override
  public boolean contains(GNode<N> src) {
    return contains(src, MethodFlag.ALL);
  }

  @Override
  public boolean contains(GNode<N> src, byte flags) {
    Node<N,E> gsrc = downcast(src);
    return gsrc.contains(this);
  }

  @Override
  public int size() {
    return size(MethodFlag.ALL);
  }

  @Override
  public int size(byte flags) {
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
    Node<N,E> gsrc = downcast(src);
    Node<N,E> gdst = downcast(dst);

    return gsrc.removeNeighbor(gdst);
  }

  @Override
  public boolean hasNeighbor(GNode<N> src, GNode<N> dst) {
    return hasNeighbor(src, dst, MethodFlag.ALL);
  }

  @Override
  public boolean hasNeighbor(GNode<N> src, GNode<N> dst, byte flags) {
    Node<N,E> gsrc = downcast(src);
    Node<N,E> gdst = downcast(dst);
    return gsrc.hasNeighbor(gdst);
  }

  @Override
  public void mapInNeighbors(GNode<N> src, LambdaVoid<GNode<N>> lambda) {
    mapInNeighbors(src, lambda, MethodFlag.ALL);
  }

  @Override
  public void mapInNeighbors(GNode<N> src, LambdaVoid<GNode<N>> body, byte flags) {
    Node<N,E> gsrc = downcast(src);
    gsrc.mapIn(body);
  }

  @Override
  public int inNeighborsSize(GNode<N> src) {
    return inNeighborsSize(src, MethodFlag.ALL);
  }

  @Override
  public int inNeighborsSize(GNode<N> src, byte flags) {
    Node<N,E> gsrc = downcast(src);
    return gsrc.inSize();
  }

  @Override
  public int outNeighborsSize(GNode<N> src) {
    return outNeighborsSize(src, MethodFlag.ALL);
  }

  @Override
  public int outNeighborsSize(GNode<N> src, byte flags) {
    Node<N,E> gsrc = downcast(src);
    return gsrc.outSize();
  }

  @Override
  public boolean addEdge(GNode<N> src, GNode<N> dst, E data) {
    return addEdge(src, dst, data, MethodFlag.ALL);
  }

  @Override
  public boolean addEdge(GNode<N> src, GNode<N> dst, final E data, byte flags) {
    Node<N,E> gsrc = downcast(src);
    Node<N,E> gdst = downcast(dst);
    if (gsrc.addEdge(gdst, data)) {
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
    Node<N,E> gsrc = downcast(src);
    Node<N,E> gdst = downcast(dst);
    E ret;
    if ((ret = gsrc.getEdgeData(gdst)) != null) {
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
    Node<N,E> gsrc = downcast(src);
    Node<N,E> gdst = downcast(dst);
    E oldData = gsrc.setEdgeData(gdst, data);

    return oldData;
  }

  @Override
  public boolean isDirected() {
    return !undirected;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void pmap(LambdaVoid<GNode<N>> body, PmapContext ctx) {
    AtomicInteger cur = (AtomicInteger) ctx.getContextObject();

    for (int tid = cur.getAndIncrement(); tid < GaloisRuntime.getRuntime().getMaxThreads(); tid = cur.getAndIncrement()) {
      LinkedNode curr = heads[getIndex(tid)];
      while (curr != null) {
        if (!curr.isDummy()) {
          Node<N,E> gsrc = (Node<N,E>) curr;
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
    for (int tid = 0; tid < GaloisRuntime.getRuntime().getMaxThreads(); tid++) {
      LinkedNode cur = heads[getIndex(tid)];
      while (cur != null) {
        if (!cur.isDummy()) {
          Node<N,E> gsrc = (Node<N,E>) cur;
          assert gsrc.in;
          body.call(gsrc);
        }
        cur = cur.getNext();
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
    for (int tid = 0; tid < GaloisRuntime.getRuntime().getMaxThreads(); tid++) {
      LinkedNode cur = heads[getIndex(tid)];
      while (cur != null) {
        if (!cur.isDummy()) {
          Node<N,E> gsrc = (Node<N,E>) cur;
          assert gsrc.in;
          body.call(gsrc, arg1);
        }
        cur = cur.getNext();
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
    for (int tid = 0; tid < GaloisRuntime.getRuntime().getMaxThreads(); tid++) {
      LinkedNode cur = heads[getIndex(tid)];
      while (cur != null) {
        if (!cur.isDummy()) {
          Node<N,E> gsrc = (Node<N,E>) cur;
          assert gsrc.in;
          body.call(gsrc, arg1, arg2);
        }
        cur = cur.getNext();
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
  
  private static class UNode<N extends GObject,E> extends Node<N,E> {
    private UNode(N d, int approxNeighbors) {
      super(d, approxNeighbors);
    }
    @Override
    protected boolean undirected() {
      return true;
    }
  }
  
  private static class Node<N extends GObject,E> extends ConcurrentGNode<N> implements LinkedNode {
    private final List<Node<N,E>> outs;
    private final List<E> outData;
    private final List<Node<N,E>> ins;
    private N data;
    private LinkedNode dummy;
    private LinkedNode next;
    private boolean in;

    private Node(N d, int approxNeighbors) {
      outs = new ArrayList<Node<N,E>>(approxNeighbors);
      outData = new ArrayList<E>(approxNeighbors);
      ins = new ArrayList<Node<N,E>>(approxNeighbors);
      data = d;
    }

    boolean contains(SerialMorphObjectGraph<N, E> g) {
      // XXX: Don't yet check if this node is in this graph
      return in;
    }

    protected boolean undirected() {
      return false;
    }
    
    LinkedNode add(LinkedNode head) {
      if (!in) {
        in = true;
        dummy = new DummyLinkedNode();
        dummy.setNext(this);

        next = head;
        return dummy;
      }

      return null;
    }

    boolean hasNeighbor(Node<N,E> node) {
      return outs.contains(node) || (undirected() && ins.contains(node));
    }

    boolean addEdge(Node<N,E> dst, E data) {
      if (!outs.contains(dst)) {
        // XXX
        if (undirected() && ins.contains(dst)) 
          return false;
//        if (true) {
//          dst.outs.add(this);
//          dst.outData.add(data);
//          ins.add(dst);
//        }
        
        outs.add(dst);
        outData.add(data);
        dst.ins.add(this);
        
        return true;
      }
      return false;
    }

    E getEdgeData(Node<N,E> dst) {
      int index = outs.indexOf(dst);
      if (index >= 0)
        return outData.get(index);
      else if (undirected() && ins.contains(dst))
        return dst.getEdgeData(this);
      else
        return null;
    }

    E setEdgeData(Node<N,E> dst, E data) {
      int index = outs.indexOf(dst);
      if (index >= 0)
        return outData.set(index, data);
      else if (undirected() && ins.contains(dst))
        return dst.setEdgeData(this, data);
      else
        throw new IllegalArgumentException("Cannot set edge data on nonexistent edge in graph");
    }

    boolean remove(SerialMorphObjectGraph<N, E> g) {
      if (!contains(g)) {
        return false;
      }
      in = false;
      dummy.setNext(next);

      // Reverse iteration to optimize List.remove() in removeNeighbor
      // *and* to provide stable list indices as we remove elements from
      // outs/outData
      for (int i = outs.size() - 1; i >= 0; i--) {
        removeNeighbor(outs.get(i), -1, i);
      }

      // Ditto
      for (int i = ins.size() - 1; i >= 0; i--) {
        ins.get(i).removeNeighbor(this, i);
      }

      return true;
    }

    boolean removeNeighbor(Node<N,E> node) {
      return removeNeighbor(node, -1, outs.indexOf(node));
    }

    private boolean removeNeighbor(Node<N,E> node, int inIndex) {
      return removeNeighbor(node, inIndex, outs.indexOf(node));
    }

    private boolean removeNeighbor(Node<N,E> node, int inIndex, int outIndex) {
      if (outIndex < 0) {
        // If not an out edge, probably an in edge. Save the in index
        // as an optimization.
        inIndex = ins.indexOf(node);
        if (inIndex < 0)
          return false;
        else if (undirected())
          return node.removeNeighbor(this, inIndex);
        return false;
      }

      // Remove outs
      int last = outs.size() - 1;
      if (outIndex != last) {
        outs.set(outIndex, outs.get(last));
        outData.set(outIndex, outData.get(last));
      }
      outs.remove(last);
      outData.remove(last);

      // Remove ins
      if (inIndex < 0)
        inIndex = node.ins.indexOf(this);

      last = node.ins.size() - 1;
      if (inIndex != last)
        node.ins.set(inIndex, node.ins.get(last));

      node.ins.remove(last);

      return true;
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
      N ret = data;
      return ret;
    }

    @Override
    public N setData(N data) {
      return setData(data, MethodFlag.ALL);
    }

    @Override
    public N setData(N data, byte flags) {
      N oldData = this.data;
      if (oldData != data) {
        this.data = data;
      }
      return oldData;
    }

    void mapIn(LambdaVoid<GNode<N>> body) {
      for (int i = 0; i < ins.size(); i++) {
        body.call(ins.get(i));
      }

      if (undirected()) {
        for (int i = 0; i < outs.size(); i++) {
          body.call(outs.get(i));
        }
      }
    }

    int inSize() {
      return undirected() ? ins.size() + outs.size() : ins.size();
    }

    int outSize() {
      return undirected() ? ins.size() + outs.size() : outs.size();
    }

    @Override
    public void map(LambdaVoid<GNode<N>> body) {
      map(body, MethodFlag.ALL);
    }

    @Override
    public void pmap(LambdaVoid<GNode<N>> body, PmapContext ctx) {
      AtomicInteger cur = (AtomicInteger) ctx.getContextObject();

      int osize = outs.size();
      int size = osize + (undirected() ? ins.size() : 0);
      for (int i = cur.getAndAdd(chunkSize); i < size; i = cur.getAndAdd(chunkSize)) {
        for (int j = 0; j < chunkSize; j++) {
          int index = i + j;
          if (index >= size) {
            break;
          }

          if (index < osize) {
            body.call(outs.get(index));
          } else {
            body.call(ins.get(index - osize));
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
    public void map(LambdaVoid<GNode<N>> body, byte flags) {
      for (int i = 0; i < outs.size(); i++) {
        body.call(outs.get(i));
      }
      if (undirected()) {
        for (int i = 0; i < ins.size(); i++) {
          body.call(ins.get(i));
        }
      }
    }

    @Override
    public <A1> void map(Lambda2Void<GNode<N>, A1> body, A1 arg1) {
      map(body, arg1, MethodFlag.ALL);
    }

    @Override
    public <A1> void map(Lambda2Void<GNode<N>, A1> body, A1 arg1, byte flags) {
      for (int i = 0; i < outs.size(); i++) {
        body.call(outs.get(i), arg1);
      }
      if (undirected()) {
        for (int i = 0; i < ins.size(); i++) {
          body.call(ins.get(i), arg1);
        }
      }
    }

    @Override
    public <A1, A2> void map(Lambda3Void<GNode<N>, A1, A2> body, A1 arg1, A2 arg2) {
      map(body, arg1, arg2, MethodFlag.ALL);
    }

    @Override
    public <A1, A2> void map(Lambda3Void<GNode<N>, A1, A2> body, A1 arg1, A2 arg2, byte flags) {
      for (int i = 0; i < outs.size(); i++) {
        body.call(outs.get(i), arg1, arg2);
      }
      if (undirected()) {
        for (int i = 0; i < ins.size(); i++) {
          body.call(ins.get(i), arg1, arg2);
        }
      }
    }
  }
  
  private class GraphIterator implements Iterator<GNode<N>> {
    private GNode<N> next;
    private LinkedNode cur;
    private int tid;
    private final int maxThreads;
    
    public GraphIterator() {
      maxThreads = GaloisRuntime.getRuntime().getMaxThreads();
      cur = heads[getIndex(tid)];
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
      while (tid < maxThreads) {
        while (cur != null) {
          if (!cur.isDummy()) {
            Node<N,E> gsrc = (Node<N,E>) cur;
            assert gsrc.in;
            next = gsrc;
            cur = cur.getNext();
            return;
          }
          cur = cur.getNext();
        }
        
        if (++tid < maxThreads)
          cur = heads[getIndex(tid)];
        else
          break;
      }

      next = null;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
