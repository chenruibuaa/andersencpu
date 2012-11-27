package galois.objects.graph;

import galois.objects.GObject;
import galois.objects.MethodFlag;
import galois.runtime.GaloisRuntime;
import galois.runtime.Iteration;
import galois.runtime.PmapContext;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import util.fn.Lambda2Void;
import util.fn.Lambda3Void;
import util.fn.LambdaVoid;

class ReaderGraph<N extends GObject> implements BuilderGraph<N,Object>, BuilderGraphData<N,Object>, Graph<N>, IntGraph<N>, LongGraph<N>, FloatGraph<N>,
    DoubleGraph<N> {
  private static final int chunkSize = 16 * GaloisRuntime.getRuntime().getMaxThreads();
  private final GraphData g;
  private final EdgeData<Object> edgeData;
  private final boolean undirected;

  public ReaderGraph(GraphData g, EdgeData<Object> edgeData, boolean undirected) {
    this.g = g;
    this.edgeData = edgeData;
    this.undirected = undirected;
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
    return edgeData.getDouble(getOutDataIdx(src, dst));
  }

  @Override
  public final float getFloatEdgeData(GNode<N> src, GNode<N> dst) {
    return getFloatEdgeData(src, dst, MethodFlag.ALL);
  }

  @Override
  public final float getFloatEdgeData(GNode<N> src, GNode<N> dst, byte flags) {
    return edgeData.getFloat(getOutDataIdx(src, dst));
  }

  @Override
  public final int getIntEdgeData(GNode<N> src, GNode<N> dst) {
    return getIntEdgeData(src, dst, MethodFlag.ALL);
  }

  @Override
  public final int getIntEdgeData(GNode<N> src, GNode<N> dst, byte flags) {
    return edgeData.getInt(getOutDataIdx(src, dst));
  }

  @Override
  public final long getLongEdgeData(GNode<N> src, GNode<N> dst) {
    return getLongEdgeData(src, dst, MethodFlag.ALL);
  }

  @Override
  public final long getLongEdgeData(GNode<N> src, GNode<N> dst, byte flags) {
    return edgeData.getLong(getOutDataIdx(src, dst));
  }

  @Override
  public BuilderGraphData<N,Object> getBuilderGraphData() {
    return this;
  }

  @Override
  public GraphData getGraphData() {
    return g;
  }

  @Override
  public NodeData<N> getNodeData() {
    return new NodeData<N>() {
      @Override
      public void set(int id, N v) {
      }
      @Override
      public N get(int id) {
        return null;
      }
    };
  }

  @Override
  public EdgeData<Object> getEdgeData() {
    return edgeData;
  }

  private GNode<N> getNode(int id) {
    return new Node(id);
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
        return getNode(cur++);
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
    for (int i = 0; i < g.getNumNodes(); i++) {
      body.call(getNode(i), arg1);
    }
  }

  @Override
  public final <A1, A2> void map(Lambda3Void<GNode<N>, A1, A2> body, A1 arg1, A2 arg2) {
    map(body, arg1, arg2, MethodFlag.ALL);
  }

  @Override
  public final <A1, A2> void map(Lambda3Void<GNode<N>, A1, A2> body, A1 arg1, A2 arg2, byte flags) {
    for (int i = 0; i < g.getNumNodes(); i++) {
      body.call(getNode(i), arg1, arg2);
    }
  }

  @Override
  public final void map(LambdaVoid<GNode<N>> body) {
    map(body, MethodFlag.ALL);
  }

  @Override
  public final void map(LambdaVoid<GNode<N>> body, byte flags) {
    for (int i = 0; i < g.getNumNodes(); i++) {
      body.call(getNode(i));
    }
  }

  @Override
  public final void mapInNeighbors(GNode<N> src, LambdaVoid<GNode<N>> body) {
    mapInNeighbors(src, body, MethodFlag.ALL);
  }

  @Override
  public final void mapInNeighbors(GNode<N> src, LambdaVoid<GNode<N>> body, byte flags) {
    int id = ((Node) src).id;
    long start = g.startIn(id);
    long end = g.endIn(id);
    for (long i = start; i < end; i++) {
      body.call(getNode(g.getInNode(i)));
    }
    if (undirected) {
      start = g.startOut(id);
      end = g.endOut(id);
      for (long i = start; i < end; i++) {
        body.call(getNode(g.getOutNode(i)));
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

    for (int i = cur.getAndAdd(chunkSize); i < g.getNumNodes(); i = cur.getAndAdd(chunkSize)) {
      for (int j = 0; j < chunkSize; j++) {
        int index = i + j;
        if (index >= g.getNumNodes()) {
          break;
        }

        body.call(getNode(index));
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
    throw new UnsupportedOperationException();
  }

  @Override
  public final double setDoubleEdgeData(GNode<N> src, GNode<N> dst, double d, byte flags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final float setFloatEdgeData(GNode<N> src, GNode<N> dst, float d) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final float setFloatEdgeData(GNode<N> src, GNode<N> dst, float d, byte flags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final int setIntEdgeData(GNode<N> src, GNode<N> dst, int d) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final int setIntEdgeData(GNode<N> src, GNode<N> dst, int d, byte flags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final long setLongEdgeData(GNode<N> src, GNode<N> dst, long d) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final long setLongEdgeData(GNode<N> src, GNode<N> dst, long d, byte flags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final int size() {
    return size(MethodFlag.ALL);
  }

  @Override
  public final int size(byte flags) {
    return g.getNumNodes();
  }

  private class Node implements GNode<N> {
    private final int id;

    public Node(int id) {
      this.id = id;
    }

    @Override
    public final void access(Iteration it, byte flags) {
    }

    @Override
    public final void afterPmap(PmapContext ctx) {
    }

    @Override
    public final void beforePmap(PmapContext ctx) {
      ctx.setContextObject(new AtomicInteger());
    }

    public final boolean contains(ReaderGraph<N> g) {
      return ReaderGraph.this == g;
    }

    @Override
    public final N getData() {
      return null;
    }

    @Override
    public final N getData(byte flags) {
      return null;
    }

    @Override
    public final N getData(byte nodeFlags, byte dataFlags) {
      return null;
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
      long start = g.startOut(id);
      long end = g.endOut(id);
      for (long i = start; i < end; i++) {
        body.call(getNode(g.getOutNode(i)), arg1);
      }
      if (undirected) {
        start = g.startIn(id);
        end = g.endIn(id);
        for (long i = start; i < end; i++) {
          body.call(getNode(g.getInNode(i)), arg1);
        }
      }
    }

    @Override
    public final <A1, A2> void map(Lambda3Void<GNode<N>, A1, A2> body, A1 arg1, A2 arg2) {
      map(body, arg1, arg2, MethodFlag.ALL);
    }

    @Override
    public final <A1, A2> void map(Lambda3Void<GNode<N>, A1, A2> body, A1 arg1, A2 arg2, byte flags) {
      long start = g.startOut(id);
      long end = g.endOut(id);
      for (long i = start; i < end; i++) {
        body.call(getNode(g.getOutNode(i)), arg1, arg2);
      }
      if (undirected) {
        start = g.startIn(id);
        end = g.endIn(id);
        for (long i = start; i < end; i++) {
          body.call(getNode(g.getInNode(i)), arg1, arg2);
        }
      }
    }

    @Override
    public final void map(LambdaVoid<GNode<N>> body) {
      map(body, MethodFlag.ALL);
    }

    @Override
    public final void map(LambdaVoid<GNode<N>> body, byte flags) {
      long start = g.startOut(id);
      long end = g.endOut(id);
      for (long i = start; i < end; i++) {
        body.call(getNode(g.getOutNode(i)));
      }
      if (undirected) {
        start = g.startIn(id);
        end = g.endIn(id);
        for (long i = start; i < end; i++) {
          body.call(getNode(g.getInNode(i)));
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
            body.call(getNode(g.getOutNode(start + index)));
          } else {
            body.call(getNode(g.getInNode(ustart + index - size)));
          }
        }
      }
    }

    @Override
    public final N setData(N d) {
      throw new UnsupportedOperationException();
    }

    @Override
    public final N setData(N d, byte flags) {
      throw new UnsupportedOperationException();
    }
  }
}
