package galois.objects.graph;

import galois.objects.GObject;

interface NodeData<N extends GObject> {
  void set(int id, N v);

  N get(int id);
}
