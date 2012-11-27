package galois.objects.graph;

import galois.objects.GObject;

/**
 * Internal methods required for efficient building of graphs.
 */
interface BuilderGraph<N extends GObject, E> {
  boolean isDirected();
  BuilderGraphData<N,E> getBuilderGraphData();
}
