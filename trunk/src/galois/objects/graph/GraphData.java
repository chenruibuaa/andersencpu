package galois.objects.graph;

interface GraphData {
  int getNumNodes();
  long getNumEdges();
  long startIn(int id);
  long endIn(int id);
  long startOut(int id);
  long endOut(int id);
  int getInNode(long idx);
  int getOutNode(long idx);
}
