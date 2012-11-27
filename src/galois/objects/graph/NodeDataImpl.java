package galois.objects.graph;
import galois.objects.GObject;

class NodeDataImpl<N extends GObject> implements NodeData<N> {
  private final N[] nodes;

  @SuppressWarnings("unchecked")
  public NodeDataImpl(int numNodes) {
    nodes = (N[]) new GObject[numNodes];
  }

  public void set(int id, N v) {
    nodes[id] = v;

  }

  public N get(int id) {
    return nodes[id];
  }
}

