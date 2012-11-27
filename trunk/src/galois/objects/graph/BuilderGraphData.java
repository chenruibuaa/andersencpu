package galois.objects.graph;

import galois.objects.GObject;

interface BuilderGraphData<N extends GObject, E> {
  GraphData getGraphData();
  EdgeData<E> getEdgeData();
  NodeData<N> getNodeData();
}

