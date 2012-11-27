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

File: LocalComputationGraph.java 

 */

package galois.objects.graph;

import galois.objects.GObject;
import galois.runtime.GaloisRuntime;
import util.fn.Lambda;

/**
 * Implementation of an {@link ObjectGraph} that is optimized for <i>local
 * computation</i> operators: operators that do not add/remove nodes and edges
 * of the graph. An attempt to modify the structure of the graph will result in
 * an {@link UnsupportedOperationException}. The most typical scenario involves
 * creating a local computation graph out of a {@link ReaderGraphs} or another
 * {@link LocalComputationGraphs}.
 * 
 * <pre>
 * IntGraph&lt;Object&gt; g = ReaderGraph.IntGraphBuilder().from(file).create();
 * IntGraph&lt;Object&gt; lcg = new LocalComputationGraph.IntGraphBuilder().from(g).create();
 * </pre>
 */
public class LocalComputationGraphs {
  private static int checkInt(long x) {
    assert x <= Integer.MAX_VALUE;
    assert x >= 0;
    return (int) x;
  }
  
  /**
   * A {@link LocalComputationGraphs} builder.
   * 
   * @param <N> Type of node data on output graph
   * @param <E> Type of edge data on output graph
   */
  private abstract static class Builder<N extends GObject, E> {
    private Lambda<Object, E> edgeDataBuilder;
    private Lambda<Integer, N> nodeDataBuilder;
    private BuilderGraph<N, E> in;
    private boolean serial;

    protected Builder() {
      serial = GaloisRuntime.getRuntime().useSerial();
    }

    protected final void check() {
      if (in == null)
        throw new UnsupportedOperationException("No graph to build from");
    }

    /**
     * Specifies the input graph.
     * 
     * @param in
     *          input graph
     */
    @SuppressWarnings("unchecked")
    protected Builder<N, E> from(DoubleGraph<?> in) {
      this.in = (BuilderGraph<N, E>) in;
      return this;
    }

    /**
     * Specifies the input graph.
     * 
     * @param in
     *          input graph
     */
    @SuppressWarnings("unchecked")
    protected Builder<N, E> from(FloatGraph<?> in) {
      this.in = (BuilderGraph<N, E>) in;
      return this;
    }

    /**
     * Specifies the input graph.
     * 
     * @param in
     *          input graph
     */
    @SuppressWarnings("unchecked")
    protected Builder<N, E> from(Graph<?> in) {
      this.in = (BuilderGraph<N, E>) in;
      return this;
    }

    /**
     * Specifies the input graph.
     * 
     * @param in
     *          input graph
     */
    @SuppressWarnings("unchecked")
    protected Builder<N, E> from(IntGraph<?> in) {
      this.in = (BuilderGraph<N,E>) in;
      return this;
    }

    /**
     * Specifies the input graph.
     * 
     * @param in
     *          input graph
     */
    @SuppressWarnings("unchecked")
    protected Builder<N, E> from(LongGraph<?> in) {
      this.in = (BuilderGraph<N, E>) in;
      return this;
    }

    /**
     * Specifies the input graph.
     * 
     * @param in
     *          input graph
     */
    @SuppressWarnings("unchecked")
    protected <F> Builder<N, E> from(ObjectGraph<?, F> in) {
      this.in = (BuilderGraph<N, E>) in;
      return this;
    }

    /**
     * Indicates whether the implementation of the graph about to be created is
     * serial (there is no concurrency or transactional support) or parallel
     * (can be safely used within Galois iterators). For example, a graph that
     * is purely thread local can benefit from using a serial implementation,
     * which is expected to add no overheads due to concurrency or the runtime
     * system.
     * 
     * @param serial
     *          boolean value that indicates whether the graph is serial or not.
     */
    protected Builder<N, E> serial(boolean serial) {
      this.serial = serial;
      return this;
    }

    /**
     * Specifies function to generate new edge data for output graph.
     * 
     * @param edgeDataBuilder
     *          function from input graph edges to output graph edge data
     * @return
     */
    protected Builder<N, E> withEdgeDataBuilder(Lambda<Object, E> edgeDataBuilder) {
      this.edgeDataBuilder = edgeDataBuilder;
      return this;
    }

    /**
     * Specifies function to generate new node data for output graph.
     * 
     * @param nodeDataBuilder function from integer id and input graph node to output graph node data
     * @return
     */
    protected Builder<N, E> withNodeDataBuilder(Lambda<Integer, N> nodeDataBuilder) {
      this.nodeDataBuilder = nodeDataBuilder;
      return this;
    }

    protected abstract EdgeData<E> newEdgeData(long numEdges);

    protected final AllGraph<N, E> createGraph() {
      BuilderGraphData<N, E> d = in.getBuilderGraphData();
      GraphData g = d.getGraphData();

      EdgeData<E> ed;
      if (edgeDataBuilder != null) {
         ed = newEdgeData(g.getNumEdges());
         EdgeData<E> orig = d.getEdgeData();
         for (long idx = 0; idx < g.getNumEdges(); idx++) {
           ed.set(idx, edgeDataBuilder.call(orig.get(idx)));
         }
      } else {
        ed = d.getEdgeData();
      }

      NodeData<N> nd;
      if (nodeDataBuilder != null) {
        nd = new NodeDataImpl<N>(g.getNumNodes());
        for (int id = 0; id < g.getNumNodes(); id++) {
          nd.set(id, nodeDataBuilder.call(id));
        }
      } else {
        nd = d.getNodeData();
      }

      if (serial) {
        return new SerialLocalComputationGraph<N, E>(g, nd, ed, !in.isDirected());
      } else {
        return new LocalComputationGraph<N,E>(g, nd, ed, !in.isDirected());
      }
    }
  }
  
  /**
   * A {@link LocalComputationGraphs} builder.
   * 
   * @param <N> Type of node data on output graph
   */
  public static class IntGraphBuilder<N extends GObject> extends Builder<N, Integer> {
    @Override
    public IntGraphBuilder<N> from(DoubleGraph<?> in) {
      super.from(in);
      return this;
    }

    @Override
    public IntGraphBuilder<N> from(FloatGraph<?> in) {
      super.from(in);
      return this;
    }

    @Override
    public IntGraphBuilder<N> from(Graph<?> in) {
      super.from(in);
      return this;
    }

    @Override
    public IntGraphBuilder<N> from(IntGraph<?> in) {
      super.from(in);
      return this;
    }

    @Override
    public IntGraphBuilder<N> from(LongGraph<?> in) {
      super.from(in);
      return this;
    }

    @Override
    public <F> IntGraphBuilder<N> from(ObjectGraph<?, F> in) {
      super.from(in);
      return this;
    }

    @Override
    public IntGraphBuilder<N> serial(boolean serial) {
      super.serial(serial);
      return this;
    }

    @Override
    public IntGraphBuilder<N> withEdgeDataBuilder(Lambda<Object, Integer> edgeDataBuilder) {
      super.withEdgeDataBuilder(edgeDataBuilder);
      return this;
    }

    @Override
    public IntGraphBuilder<N> withNodeDataBuilder(Lambda<Integer, N> nodeDataBuilder) {
      super.withNodeDataBuilder(nodeDataBuilder);
      return this;
    }

    @Override
    protected EdgeData<Integer> newEdgeData(long numEdges) {
      final int[] data = new int[checkInt(numEdges)];
      return new AbstractEdgeData<Integer>() {
        @Override
        public void setInt(long idx, int v) {
          data[checkInt(idx)] = v;
        }
        @Override
        public int getInt(long idx) {
          return data[checkInt(idx)];
        }
        @Override
        public void set(long idx, Object v) {
          data[checkInt(idx)] = (Integer) v;
        }
        @Override
        public Object get(long idx) {
          return data[checkInt(idx)];
        }
      };
    }

    public IntGraph<N> create() {
      return createGraph();
    }
  }
  
  /**
   * A {@link LocalComputationGraphs} builder.
   * 
   * @param <N> Type of node data on output graph
   */
  public static class GraphBuilder<N extends GObject> extends Builder<N, Object> {
    @Override
    public GraphBuilder<N> from(DoubleGraph<?> in) {
      super.from(in);
      return this;
    }

    @Override
    public GraphBuilder<N> from(FloatGraph<?> in) {
      super.from(in);
      return this;
    }

    @Override
    public GraphBuilder<N> from(IntGraph<?> in) {
      super.from(in);
      return this;
    }

    @Override
    public GraphBuilder<N> from(Graph<?> in) {
      super.from(in);
      return this;
    }

    @Override
    public GraphBuilder<N> from(LongGraph<?> in) {
      super.from(in);
      return this;
    }

    @Override
    public <F> GraphBuilder<N> from(ObjectGraph<?, F> in) {
      super.from(in);
      return this;
    }

    @Override
    public GraphBuilder<N> serial(boolean serial) {
      super.serial(serial);
      return this;
    }

    @Override
    public GraphBuilder<N> withEdgeDataBuilder(Lambda<Object, Object> edgeDataBuilder) {
      super.withEdgeDataBuilder(edgeDataBuilder);
      return this;
    }

    @Override
    public GraphBuilder<N> withNodeDataBuilder(Lambda<Integer, N> nodeDataBuilder) {
      super.withNodeDataBuilder(nodeDataBuilder);
      return this;
    }

    @Override
    protected EdgeData<Object> newEdgeData(long numEdges) {
      return new AbstractEdgeData<Object>() {
        @Override
        public void set(long idx, Object v) {
        }
        @Override
        public Object get(long idx) {
          return null;
        }
      };
    }


    public Graph<N> create() {
      return createGraph();
    }
  }

  /**
   * A {@link LocalComputationGraphs} builder.
   * 
   * @param <N> Type of node data on output graph
   * @param <E> Type of edge data on output graph
   */
  public static class ObjectGraphBuilder<N extends GObject, E> extends Builder<N, E> {
    @Override
    public ObjectGraphBuilder<N, E> from(DoubleGraph<?> in) {
      super.from(in);
      return this;
    }

    @Override
    public ObjectGraphBuilder<N, E> from(FloatGraph<?> in) {
      super.from(in);
      return this;
    }

    @Override
    public ObjectGraphBuilder<N, E> from(IntGraph<?> in) {
      super.from(in);
      return this;
    }

    @Override
    public ObjectGraphBuilder<N, E> from(Graph<?> in) {
      super.from(in);
      return this;
    }

    @Override
    public ObjectGraphBuilder<N, E> from(LongGraph<?> in) {
      super.from(in);
      return this;
    }

    @Override
    public <F> ObjectGraphBuilder<N, E> from(ObjectGraph<?, F> in) {
      super.from(in);
      return this;
    }

    @Override
    public ObjectGraphBuilder<N, E> serial(boolean serial) {
      super.serial(serial);
      return this;
    }

    @Override
    public ObjectGraphBuilder<N, E> withEdgeDataBuilder(Lambda<Object,E> edgeDataBuilder) {
      super.withEdgeDataBuilder(edgeDataBuilder);
      return this;
    }

    @Override
    public ObjectGraphBuilder<N, E> withNodeDataBuilder(Lambda<Integer, N> nodeDataBuilder) {
      super.withNodeDataBuilder(nodeDataBuilder);
      return this;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    protected EdgeData<E> newEdgeData(long numEdges) {
      final E[] data = (E[]) new Object[checkInt(numEdges)];
      return new AbstractEdgeData<E>() {
        @Override
        public void setObject(long idx, E v) {
          data[checkInt(idx)] = v;
        }
        @Override
        public E getObject(long idx) {
          return data[checkInt(idx)];
        }
        @Override
        public void set(long idx, Object v) {
          data[checkInt(idx)] = (E) v;
        }
        @Override
        public Object get(long idx) {
          return data[checkInt(idx)];
        }
      };
    }

    public ObjectGraph<N, E> create() {
      return createGraph();
    }
  }
}
