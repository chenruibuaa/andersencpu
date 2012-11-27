package galois.objects.graph;

import galois.objects.GObject;
import galois.objects.MethodFlag;
import galois.runtime.GaloisRuntime;
import galois.runtime.Iteration;
import galois.runtime.PmapContext;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import util.fn.Lambda0Void;
import util.fn.Lambda2Void;
import util.fn.Lambda3Void;
import util.fn.LambdaVoid;

/**
 * 
 */
class LocalComputationGraph<N extends GObject, E> implements BuilderGraph<N,E>, BuilderGraphData<N,E>, AllGraph<N,E> {
  private static final int chunkSize = 16 * GaloisRuntime.getRuntime().getMaxThreads();
  private final GraphData g;
  private final NodeData<N> nodeData;
  private final EdgeData<E> edgeData;
  private final boolean undirected;
  private final Node[] nodes;

  @SuppressWarnings("unchecked")
  public LocalComputationGraph(GraphData g, NodeData<N> nodeData, EdgeData<E> edgeData, boolean undirected) {
    this.g = g;
    this.nodeData = nodeData;
    this.edgeData = edgeData;
    this.undirected = undirected;
    nodes = new LocalComputationGraph.Node[g.getNumNodes()];
    for (int i = 0; i < nodes.length; i++) {
      nodes[i] = new Node(i);
    }
  }

  protected final int getNumNodes() {
    return g.getNumNodes();
  }

  protected final long getNumEdges() {
    return g.getNumEdges();
  }

  private static void acquireAll(byte flags) {
    if (GaloisRuntime.needMethodFlag(flags, MethodFlag.CHECK_CONFLICT)) {
      throw new UnsupportedOperationException();
    }
  }

  private void acquireEdge(GNode<N> src, GNode<N> dst, byte flags) {
    Iteration it = Iteration.access(src, flags);
    Iteration.access(it, dst, flags);
  }

  void acquireNeighbors(GNode<N> src, boolean isIn, byte flags) {
    Node nsrc = (Node) src;
    Iteration it = Iteration.access(nsrc, flags);

    if (it != null) {
      long start = isIn ? g.startIn(nsrc.id) : g.startOut(nsrc.id);
      long end = isIn ? g.endIn(nsrc.id) : g.endOut(nsrc.id);

      for (long i = start; i < end; i++) {
        Node other = nodes[isIn ? g.getInNode(i) : g.getOutNode(i)];
        Iteration.access(it, other, flags);
      }
    }
  }

  @Override
  public final boolean add(GNode<N> n) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final boolean add(GNode<N> n, byte flags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final boolean addEdge(GNode<N> src, GNode<N> dst, double data) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final boolean addEdge(GNode<N> src, GNode<N> dst, double data, byte flags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addEdge(GNode<N> src, GNode<N> dst, E data) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addEdge(GNode<N> src, GNode<N> dst, E data, byte flags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final boolean addEdge(GNode<N> src, GNode<N> dst, float data) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final boolean addEdge(GNode<N> src, GNode<N> dst, float data, byte flags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final boolean addEdge(GNode<N> src, GNode<N> dst, int data) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final boolean addEdge(GNode<N> src, GNode<N> dst, int data, byte flags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final boolean addEdge(GNode<N> src, GNode<N> dst, long data) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final boolean addEdge(GNode<N> src, GNode<N> dst, long data, byte flags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final boolean addNeighbor(GNode<N> src, GNode<N> dst) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final boolean addNeighbor(GNode<N> src, GNode<N> dst, byte flags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void afterPmap(PmapContext ctx) {
  }

  @Override
  public final void beforePmap(PmapContext ctx) {
    ctx.setContextObject(new AtomicInteger());
  }

  @Override
  public final boolean contains(GNode<N> n) {
    return contains(n, MethodFlag.ALL);
  }

  @Override
  public final boolean contains(GNode<N> n, byte flags) {
    Iteration.access(n, flags);
    return ((Node) n).contains(this);
  }

  @Override
  public final GNode<N> createNode(N n) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final GNode<N> createNode(N n, byte flags) {
    throw new UnsupportedOperationException();
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
  public final double getDoubleEdgeData(GNode<N> src, GNode<N> dst) {
    return getDoubleEdgeData(src, dst, MethodFlag.ALL);
  }

  @Override
  public final double getDoubleEdgeData(GNode<N> src, GNode<N> dst, byte flags) {
    acquireEdge(src, dst, flags);
    return edgeData.getDouble(getOutDataIdx(src, dst));
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
    Iteration it = Iteration.access(src, edgeFlags);
    Iteration.access(it, dst, edgeFlags);
    E retval = edgeData.getObject(getOutDataIdx(src, dst));
    Iteration.access(it, retval, dataFlags);
    return retval;
  }

  @Override
  public final float getFloatEdgeData(GNode<N> src, GNode<N> dst) {
    return getFloatEdgeData(src, dst, MethodFlag.ALL);
  }

  @Override
  public final float getFloatEdgeData(GNode<N> src, GNode<N> dst, byte flags) {
    acquireEdge(src, dst, flags);
    return edgeData.getFloat(getOutDataIdx(src, dst));
  }

  @Override
  public final int getIntEdgeData(GNode<N> src, GNode<N> dst) {
    return getIntEdgeData(src, dst, MethodFlag.ALL);
  }

  @Override
  public final int getIntEdgeData(GNode<N> src, GNode<N> dst, byte flags) {
    acquireEdge(src, dst, flags);
    return edgeData.getInt(getOutDataIdx(src, dst));
  }

  @Override
  public final long getLongEdgeData(GNode<N> src, GNode<N> dst) {
    return getLongEdgeData(src, dst, MethodFlag.ALL);
  }

  @Override
  public final long getLongEdgeData(GNode<N> src, GNode<N> dst, byte flags) {
    acquireEdge(src, dst, flags);
    return edgeData.getLong(getOutDataIdx(src, dst));
  }

  @Override
  public BuilderGraphData<N,E> getBuilderGraphData() {
    return this;
  }

  @Override
  public GraphData getGraphData() {
    return g;
  }

  @Override
  public NodeData<N> getNodeData() {
    return nodeData;
  }

  @Override
  public EdgeData<E> getEdgeData() {
    return edgeData;
  }

  private long getOutDataIdx(GNode<N> src, GNode<N> dst) {
    int sid = ((Node) src).id();
    int did = ((Node) dst).id();
    long retval;
    if ((retval = getOutDataIdx(sid, did)) >= 0)
      return retval;

    if (undirected) {
      long start = g.startIn(sid);
      long end = g.endIn(sid);
      for (long i = start; i < end; i++) {
        if (g.getInNode(i) == did) {
          if ((retval = getOutDataIdx(did, sid)) >= 0)
            return retval;
          break;
        }
      }
    }

    throw new IndexOutOfBoundsException();
  }

  private long getOutDataIdx(int sid, int did) {
    long start = g.startOut(sid);
    long end = g.endOut(sid);
    for (long i = start; i < end; i++) {
      if (g.getOutNode(i) == did)
        return i;
    }
    return -1;
  }

  @Override
  public final boolean hasNeighbor(GNode<N> src, GNode<N> dst) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final boolean hasNeighbor(GNode<N> src, GNode<N> dst, byte flags) {
    Node nsrc = (Node) src;
    Node ndst = (Node) dst;
    Iteration it = Iteration.access(nsrc, flags);
    Iteration.access(it, ndst, flags);
    long start = g.startOut(nsrc.id);
    long end = g.endOut(nsrc.id);
    for (long i = start; i < end; i++) {
      if (g.getOutNode(i) == ndst.id)
        return true;
    }
    start = g.startIn(nsrc.id);
    end = g.endIn(nsrc.id);
    for (long i = start; i < end; i++) {
      if (g.getInNode(i) == ndst.id)
        return true;
    }

    return false;
  }

  @Override
  public final int inNeighborsSize(GNode<N> src) {
    return inNeighborsSize(src, MethodFlag.ALL);
  }

  @Override
  public final int inNeighborsSize(GNode<N> src, byte flags) {
    Node n = (Node) src;
    int inSize = inNeighborsSize(n);
    if (undirected) {
      return inSize + outNeighborsSize(n);
    } else {
      return inSize;
    }
  }

  private int inNeighborsSize(Node n) {
    long start = g.startIn(n.id);
    long end = g.endIn(n.id);
    long size = end - start;
    assert size <= Integer.MAX_VALUE && size >= 0;
    return (int) size;
  }

  @Override
  public final boolean isDirected() {
    return !undirected;
  }

  @Override
  public final Iterator<GNode<N>> iterator() {
    return new Iterator<GNode<N>>() {
      private int cur;

      @Override
      public final boolean hasNext() {
        return cur < g.getNumNodes();
      }

      @Override
      public final GNode<N> next() {
        return nodes[cur++];
      }

      @Override
      public final void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public final <A1> void map(Lambda2Void<GNode<N>, A1> body, A1 arg1) {
    map(body, arg1, MethodFlag.ALL);
  }

  @Override
  public final <A1> void map(Lambda2Void<GNode<N>, A1> body, A1 arg1, byte flags) {
    acquireAll(flags);
    for (int i = 0; i < nodes.length; i++) {
      body.call(nodes[i], arg1);
    }
  }

  @Override
  public final <A1, A2> void map(Lambda3Void<GNode<N>, A1, A2> body, A1 arg1, A2 arg2) {
    map(body, arg1, arg2, MethodFlag.ALL);
  }

  @Override
  public final <A1, A2> void map(Lambda3Void<GNode<N>, A1, A2> body, A1 arg1, A2 arg2, byte flags) {
    acquireAll(flags);
    for (int i = 0; i < nodes.length; i++) {
      body.call(nodes[i], arg1, arg2);
    }
  }

  @Override
  public final void map(LambdaVoid<GNode<N>> body) {
    map(body, MethodFlag.ALL);
  }

  @Override
  public final void map(LambdaVoid<GNode<N>> body, byte flags) {
    acquireAll(flags);
    for (int i = 0; i < nodes.length; i++) {
      body.call(nodes[i]);
    }
  }

  @Override
  public final void mapInNeighbors(GNode<N> src, LambdaVoid<GNode<N>> body) {
    mapInNeighbors(src, body, MethodFlag.ALL);
  }

  @Override
  public final void mapInNeighbors(GNode<N> src, LambdaVoid<GNode<N>> body, byte flags) {
    acquireNeighbors(src, true, flags);
    if (undirected) {
      acquireNeighbors(src, false, flags);
    }
    
    int id = ((Node) src).id;
    long start = g.startIn(id);
    long end = g.endIn(id);
    for (long i = start; i < end; i++) {
      body.call(nodes[g.getInNode(i)]);
    }
    if (undirected) {
      start = g.startOut(id);
      end = g.endOut(id);
      for (long i = start; i < end; i++) {
        body.call(nodes[g.getOutNode(i)]);
      }
    }
  }

  @Override
  public final int outNeighborsSize(GNode<N> src) {
    return outNeighborsSize(src, MethodFlag.ALL);
  }

  @Override
  public final int outNeighborsSize(GNode<N> src, byte flags) {
    Node n = (Node) src;
    int outSize = outNeighborsSize(n);
    if (undirected) {
      return outSize + inNeighborsSize(n);
    } else {
      return outSize;
    }
  }
  
  private int outNeighborsSize(Node n) {
    long start = g.startOut(n.id);
    long end = g.endOut(n.id);
    long size = end - start;
    assert size <= Integer.MAX_VALUE && size >= 0;
    return (int) size;
  }
  
  @Override
  public final void pmap(LambdaVoid<GNode<N>> body, PmapContext ctx) {
    AtomicInteger cur = (AtomicInteger) ctx.getContextObject();

    for (int i = cur.getAndAdd(chunkSize); i < nodes.length; i = cur.getAndAdd(chunkSize)) {
      for (int j = 0; j < chunkSize; j++) {
        int index = i + j;
        if (index >= nodes.length) {
          break;
        }

        body.call(nodes[index]);
      }
    }
  }

  @Override
  public final boolean remove(GNode<N> n) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final boolean remove(GNode<N> n, byte flags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final boolean removeNeighbor(GNode<N> src, GNode<N> dst) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final boolean removeNeighbor(GNode<N> src, GNode<N> dst, byte flags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final double setDoubleEdgeData(GNode<N> src, GNode<N> dst, double d) {
    return setDoubleEdgeData(src, dst, d, MethodFlag.ALL);
  }

  @Override
  public final double setDoubleEdgeData(GNode<N> src, GNode<N> dst, double d, byte flags) {
    Iteration it = Iteration.access(src, flags);
    Iteration.access(it, dst, flags);

    final long idx = getOutDataIdx(src, dst);
    final double oldData = edgeData.getDouble(idx);

    if (oldData != d) {
      edgeData.setDouble(idx, d);
      if (GaloisRuntime.needMethodFlag(flags, MethodFlag.SAVE_UNDO)) {
        GaloisRuntime.getRuntime().onUndo(Iteration.getCurrentIteration(), new Lambda0Void() {
          @Override
          public void call() {
            edgeData.setDouble(idx, oldData);
          }
        });
      }
    }

    return oldData;
  }

  @Override
  public E setEdgeData(GNode<N> src, GNode<N> dst, E d) {
    return setEdgeData(src, dst, d, MethodFlag.ALL);
  }

  @Override
  public E setEdgeData(GNode<N> src, GNode<N> dst, E d, byte flags) {
    Iteration it = Iteration.access(src, flags);
    Iteration.access(it, dst, flags);

    final long idx = getOutDataIdx(src, dst);
    final E oldData = edgeData.getObject(idx);

    if (oldData != d) {
      edgeData.setObject(idx, d);
      if (GaloisRuntime.needMethodFlag(flags, MethodFlag.SAVE_UNDO)) {
        GaloisRuntime.getRuntime().onUndo(Iteration.getCurrentIteration(), new Lambda0Void() {
          @Override
          public void call() {
            edgeData.setObject(idx, oldData);
          }
        });
      }
    }

    return oldData;
  }

  @Override
  public final float setFloatEdgeData(GNode<N> src, GNode<N> dst, float d) {
    return setFloatEdgeData(src, dst, d, MethodFlag.ALL);
  }

  @Override
  public final float setFloatEdgeData(GNode<N> src, GNode<N> dst, float d, byte flags) {
    Iteration it = Iteration.access(src, flags);
    Iteration.access(it, dst, flags);

    final long idx = getOutDataIdx(src, dst);
    final float oldData = edgeData.getFloat(idx);

    if (oldData != d) {
      edgeData.setFloat(idx, d);
      if (GaloisRuntime.needMethodFlag(flags, MethodFlag.SAVE_UNDO)) {
        GaloisRuntime.getRuntime().onUndo(Iteration.getCurrentIteration(), new Lambda0Void() {
          @Override
          public void call() {
            edgeData.setFloat(idx, oldData);
          }
        });
      }
    }

    return oldData;
  }

  @Override
  public final int setIntEdgeData(GNode<N> src, GNode<N> dst, int d) {
    return setIntEdgeData(src, dst, d, MethodFlag.ALL);
  }

  @Override
  public final int setIntEdgeData(GNode<N> src, GNode<N> dst, int d, byte flags) {
    Iteration it = Iteration.access(src, flags);
    Iteration.access(it, dst, flags);

    final long idx = getOutDataIdx(src, dst);
    final int oldData = edgeData.getInt(idx);

    if (oldData != d) {
      edgeData.setInt(idx, d);
      if (GaloisRuntime.needMethodFlag(flags, MethodFlag.SAVE_UNDO)) {
        GaloisRuntime.getRuntime().onUndo(Iteration.getCurrentIteration(), new Lambda0Void() {
          @Override
          public void call() {
            edgeData.setInt(idx, oldData);
          }
        });
      }
    }

    return oldData;
  }

  @Override
  public final long setLongEdgeData(GNode<N> src, GNode<N> dst, long d) {
    return setLongEdgeData(src, dst, d, MethodFlag.ALL);
  }

  @Override
  public final long setLongEdgeData(GNode<N> src, GNode<N> dst, long d, byte flags) {
    Iteration it = Iteration.access(src, flags);
    Iteration.access(it, dst, flags);

    final long idx = getOutDataIdx(src, dst);
    final long oldData = edgeData.getLong(idx);

    if (oldData != d) {
      edgeData.setLong(idx, d);
      if (GaloisRuntime.needMethodFlag(flags, MethodFlag.SAVE_UNDO)) {
        GaloisRuntime.getRuntime().onUndo(Iteration.getCurrentIteration(), new Lambda0Void() {
          @Override
          public void call() {
            edgeData.setLong(idx, oldData);
          }
        });
      }
    }

    return oldData;
  }

  @Override
  public final int size() {
    return size(MethodFlag.ALL);
  }

  @Override
  public final int size(byte flags) {
    return g.getNumNodes();
  }

  protected class Node extends ConcurrentGNode<N> {
    private final int id;

    public Node(int id) {
      this.id = id;
    }

    @Override
    public final void afterPmap(PmapContext ctx) {
    }

    @Override
    public final void beforePmap(PmapContext ctx) {
      ctx.setContextObject(new AtomicInteger());
    }

    final boolean contains(LocalComputationGraph<N, E> g) {
      return LocalComputationGraph.this == g;
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
      Iteration it = Iteration.access(this, nodeFlags);
      Iteration.access(it, nodeData.get(id), dataFlags);
      return nodeData.get(id);
    }

    public final int id() {
      return id;
    }

    @Override
    public final <A1> void map(Lambda2Void<GNode<N>, A1> body, A1 arg1) {
      map(body, arg1, MethodFlag.ALL);
    }

    @Override
    public final <A1> void map(Lambda2Void<GNode<N>, A1> body, A1 arg1, byte flags) {
      acquireNeighbors(this, false, flags);
      if (undirected) {
        acquireNeighbors(this, true, flags);
      }
      
      long start = g.startOut(id);
      long end = g.endOut(id);
      for (long i = start; i < end; i++) {
        body.call(nodes[g.getOutNode(i)], arg1);
      }
      if (undirected) {
        start = g.startIn(id);
        end = g.endIn(id);
        for (long i = start; i < end; i++) {
          body.call(nodes[g.getInNode(i)], arg1);
        }
      }
    }

    @Override
    public final <A1, A2> void map(Lambda3Void<GNode<N>, A1, A2> body, A1 arg1, A2 arg2) {
      map(body, arg1, arg2, MethodFlag.ALL);
    }

    @Override
    public final <A1, A2> void map(Lambda3Void<GNode<N>, A1, A2> body, A1 arg1, A2 arg2, byte flags) {
      acquireNeighbors(this, false, flags);
      if (undirected) {
        acquireNeighbors(this, true, flags);
      }
      
      long start = g.startOut(id);
      long end = g.endOut(id);
      for (long i = start; i < end; i++) {
        body.call(nodes[g.getOutNode(i)], arg1, arg2);
      }
      if (undirected) {
        start = g.startIn(id);
        end = g.endIn(id);
        for (long i = start; i < end; i++) {
          body.call(nodes[g.getInNode(i)], arg1, arg2);
        }
      }
    }

    @Override
    public final void map(LambdaVoid<GNode<N>> body) {
      map(body, MethodFlag.ALL);
    }

    @Override
    public final void map(LambdaVoid<GNode<N>> body, byte flags) {
      acquireNeighbors(this, false, flags);
      if (undirected) {
        acquireNeighbors(this, true, flags);
      }
      
      long start = g.startOut(id);
      long end = g.endOut(id);
      for (long i = start; i < end; i++) {
        body.call(nodes[g.getOutNode(i)]);
      }
      if (undirected) {
        start = g.startIn(id);
        end = g.endIn(id);
        for (long i = start; i < end; i++) {
          body.call(nodes[g.getInNode(i)]);
        }
      }
    }

    @Override
    public final void pmap(LambdaVoid<GNode<N>> body, PmapContext ctx) {
      AtomicInteger cur = (AtomicInteger) ctx.getContextObject();
      long start = g.startOut(id);
      long ustart = g.startIn(id);

      long size = g.endOut(id) - start;
      long usize = size + g.endIn(id) - ustart;
      
      for (int i = cur.getAndAdd(chunkSize); i < usize; i = cur.getAndAdd(chunkSize)) {
        for (int j = 0; j < chunkSize; j++) {
          int index = i + j;
          if (index >= usize) {
            break;
          }

          if (index < size) {
            body.call(nodes[g.getOutNode(start + index)]);
          } else {
            body.call(nodes[g.getInNode(ustart + index - size)]);
          }
        }
      }
    }

    @Override
    public N setData(N data) {
      return setData(data, MethodFlag.ALL);
    }

    @Override
    public N setData(N data, byte flags) {
      Iteration.access(this, flags);

      final N oldData = nodeData.get(id);

      if (oldData != data) {
        nodeData.set(id, data);

        if (GaloisRuntime.needMethodFlag(flags, MethodFlag.SAVE_UNDO)) {
          GaloisRuntime.getRuntime().onUndo(Iteration.getCurrentIteration(), new Lambda0Void() {
            @Override
            public void call() {
              nodeData.set(id, oldData);
            }
          });
        }
      }

      return oldData;
    }
  }
}
