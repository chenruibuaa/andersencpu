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

File: MorphGraph.java 

 */

package galois.objects.graph;

import galois.objects.GObject;
import galois.runtime.GaloisRuntime;

import java.util.ArrayList;
import java.util.List;

import util.fn.Lambda;

/**
 * Most general implementation of a {@link ObjectGraph}, allowing modifications
 * of the structure of the graph (unlike {@link LocalComputationGraphs}), as well
 * as modifications of the data in the edges and nodes.
 * 
 * @see galois.objects.graph.LocalComputationGraphs
 */
public final class MorphGraphs {
  private MorphGraphs() {
  }

  private static class Builder<N extends GObject, E> {
    protected boolean serial;
    protected Boolean undirected;
    protected BuilderGraph<N,E> in;
    protected Lambda<Integer, N> nodeDataBuilder;
    protected Lambda<Object, E> edgeDataBuilder;

    protected Builder() {
      serial = GaloisRuntime.getRuntime().useSerial();
    }

    protected Builder(Builder<N, E> b) {
      serial = b.serial;
      undirected = b.undirected;
      in = b.in;
      nodeDataBuilder = b.nodeDataBuilder;
      edgeDataBuilder = b.edgeDataBuilder;
    }

    /**
     * Specifies the implementation of the graph about to be created is serial
     * (there is no concurrency or transactional support) or parallel (can be
     * safely used within Galois iterators). For example, a graph that is purely
     * thread local can benefit from using a serial implementation, which is
     * expected to add no overheads due to concurrency or the runtime system.
     * 
     * @param serial
     *          flag indicating whether the graph is serial or not.
     */
    protected Builder<N, E> serial(boolean serial) {
      this.serial = serial;
      return this;
    }

    /**
     * Specifies if the edges in the graph are undirected.
     * 
     * @param undirected
     *          flag indicating whether the graph is undirected.
     */
    protected Builder<N, E> undirected(boolean undirected) {
      this.undirected = undirected;
      return this;
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
     * Specifies function to generate new edge data for output graph.
     * 
     * @param <F>
     *          Type of edge data on input graph
     * @param edgeDataBuilder
     *          function from input graph edges to output graph edge data
     * 
     * @return
     */
    protected Builder<N, E> withEdgeDataBuilder(Lambda<Object, E> edgeDataBuilder) {
      this.edgeDataBuilder = edgeDataBuilder;
      return this;
    }

    /**
     * Specifies function to generate new node data for output graph.
     * 
     * @param nodeDataBuilder
     *          function from integer id and input graph node to output graph
     *          node data
     * @return
     */
    protected Builder<N, E> withNodeDataBuilder(Lambda<Integer, N> nodeDataBuilder) {
      this.nodeDataBuilder = nodeDataBuilder;
      return this;
    }
  }

  public static class ObjectGraphBuilder<N extends GObject, E> extends Builder<N, E> {
    public ObjectGraphBuilder() {
      super();
    }

    @Override
    public ObjectGraphBuilder<N, E> serial(boolean serial) {
      super.serial(serial);
      return this;
    }

    protected ObjectGraphBuilder(Builder<N, E> b) {
      super(b);
    }

    @Override
    public ObjectGraphBuilder<N, E> undirected(boolean undirected) {
      super.undirected(undirected);
      return this;
    }

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
    public ObjectGraphBuilder<N, E> from(LongGraph<?> in) {
      super.from(in);
      return this;
    }

    @Override
    public ObjectGraphBuilder<N, E> from(Graph<?> in) {
      super.from(in);
      return this;
    }

    @Override
    public <F> ObjectGraphBuilder<N, E> from(ObjectGraph<?, F> in) {
      super.from(in);
      return this;
    }

    @Override
    public ObjectGraphBuilder<N, E> withEdgeDataBuilder(Lambda<Object, E> edgeDataBuilder) {
      super.withEdgeDataBuilder(edgeDataBuilder);
      return this;
    }

    @Override
    public ObjectGraphBuilder<N, E> withNodeDataBuilder(Lambda<Integer, N> nodeDataBuilder) {
      super.withNodeDataBuilder(nodeDataBuilder);
      return this;
    }

    private ObjectGraph<N, E> makeNew() {
      boolean v;
      if (in != null && undirected != null)
        throw new Error("undirected and from options are mutually exclusive");
      else if (in != null)
        v = !in.isDirected();
      else if (undirected == null)
        v = false;
      else
        v = undirected;
      
      if (serial) {
        return new SerialMorphObjectGraph<N, E>(v);
      } else {
        return new MorphObjectGraph<N, E>(v);
      }
    }

    @SuppressWarnings("unchecked")
    public ObjectGraph<N, E> create() {
      ObjectGraph<N, E> retval = makeNew();
      if (in == null)
        return retval;
      
      BuilderGraphData<N, E> gd = in.getBuilderGraphData();
      NodeData<N> nd = gd.getNodeData();
      EdgeData<E> ed = gd.getEdgeData();
      GraphData g = gd.getGraphData();
      
      List<GNode<N>> nodes = new ArrayList<GNode<N>>(g.getNumNodes());
      for (int id = 0; id < g.getNumNodes(); id++) {
        N data = nodeDataBuilder != null ? nodeDataBuilder.call(id) : nd.get(id);
        int outSize = (int) (g.endOut(id) - g.startOut(id));
        GNode<N> node = retval.createNode(data, outSize);
        nodes.add(node);
        retval.add(node);
      }
      
      for (int id = 0; id < g.getNumNodes(); id++) {
        long start = g.startOut(id);
        long end = g.endOut(id);
        GNode<N> src = nodes.get(id);
        for (long idx = start; idx < end; idx++) {
          GNode<N> dst = nodes.get(g.getOutNode(idx));
          E data = edgeDataBuilder != null ? edgeDataBuilder.call(ed.get(idx)) : (E) ed.get(idx);
          retval.addEdge(src, dst, data);
        }
      }
      
     return retval;
    }
  }

  public static class IntGraphBuilder<N extends GObject> extends Builder<N, Integer> {
    private Integer numNodes;
    
    public IntGraphBuilder() {
      super();
    }

    @Override
    public IntGraphBuilder<N> serial(boolean serial) {
      super.serial(serial);
      return this;
    }

    @Override
    public IntGraphBuilder<N> undirected(boolean undirected) {
      super.undirected(undirected);
      return this;
    }

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
    public IntGraphBuilder<N> from(Graph<?> in) {
      super.from(in);
      return this;
    }

    @Override
    public <F> IntGraphBuilder<N> from(ObjectGraph<?, F> in) {
      super.from(in);
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

    public IntGraphBuilder<N> refinement(int numNodes) {
      this.numNodes = numNodes;
      return this;
    }
    
    public IntGraph<N> create() {
      if (numNodes == null) {
        ObjectGraph<N, Integer> g = new ObjectGraphBuilder<N, Integer>(this).create();
        return new ObjectGraphToIntGraphAdapter<N>(g);
      } else {
        if (nodeDataBuilder == null && edgeDataBuilder == null && in == null && undirected == null) {
          if (serial) {
            return new SerialRefinementIntGraph<N>(numNodes);
          } else {
            return new RefinementIntGraph<N>(numNodes);
          }
        } else {
          // TODO(ddn): implement this case
          throw new UnsupportedOperationException();
        }
      }
    }
  }

  public static class GraphBuilder<N extends GObject> extends Builder<N, Object> {
    public GraphBuilder() {
      super();
    }

    @Override
    public GraphBuilder<N> serial(boolean serial) {
      super.serial(serial);
      return this;
    }

    @Override
    public GraphBuilder<N> undirected(boolean undirected) {
      super.undirected(undirected);
      return this;
    }

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
    public GraphBuilder<N> from(LongGraph<?> in) {
      super.from(in);
      return this;
    }

    @Override
    public GraphBuilder<N> from(Graph<?> in) {
      super.from(in);
      return this;
    }

    @Override
    public <F> GraphBuilder<N> from(ObjectGraph<?, F> in) {
      super.from(in);
      return this;
    }

    @Override
    public GraphBuilder<N> withNodeDataBuilder(Lambda<Integer, N> nodeDataBuilder) {
      super.withNodeDataBuilder(nodeDataBuilder);
      return this;
    }

    public Graph<N> create() {
      ObjectGraph<N, Object> g = new ObjectGraphBuilder<N, Object>(this).create();
      return new ObjectGraphToVoidGraphAdapter<N>(g);
    }
  }
}
