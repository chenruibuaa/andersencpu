package galois.objects.graph;

import gnu.trove.list.array.TIntArrayList;

class WithInEdgesGraphData implements GraphData {
  private final GraphData g;
  private final long[] inIdxs;
  private final int[] ins;

  public WithInEdgesGraphData(GraphData g) {
    this.g = g;
    TIntArrayList[] transpose = transpose();
    int numInEdges = numInEdges(transpose);
    if (numInEdges == 0) {
      // Special case
      inIdxs = null;
      ins = null;
    } else {
      inIdxs = new long[g.getNumNodes()];
      ins = new int[numInEdges];
      fillIns(transpose);
    }
  }

  private void fillIns(TIntArrayList[] transpose) {
    int edgeIdx = 0;
    for (int i = 0; i < g.getNumNodes(); i++) {
      TIntArrayList l = transpose[i];
      for (int j = 0; j < l.size(); j++) {
        ins[edgeIdx++] = l.get(j);
      }
      inIdxs[i] = edgeIdx;
    }
  }

  private int numInEdges(TIntArrayList[] transpose) {
    int retval = 0;
    for (int i = 0; i < transpose.length; i++) {
      retval += transpose[i].size();
    }
    return retval;
  }

  private TIntArrayList[] transpose() {
    TIntArrayList[] transpose = new TIntArrayList[g.getNumNodes()];
    for (int i = 0; i < transpose.length; i++) {
      transpose[i] = new TIntArrayList();
    }

    for (int src = 0; src < g.getNumNodes(); src++) {
      long start = startOut(src);
      long end = endOut(src);
      for (long idx = start; idx < end; idx++) {
        int dst = getOutNode(idx);
        transpose[dst].add(src);
      }
    }
    return transpose;
  }

  private static int checkInt(long x) {
    assert x <= Integer.MAX_VALUE;
    assert x >= 0;
    return (int) x;
  }

  @Override
  public int getNumNodes() {
    return g.getNumNodes();
  }

  @Override
  public long getNumEdges() {
    return g.getNumEdges();
  }

  @Override
  public long startOut(int id) {
    return g.startOut(id);
  }

  @Override
  public final long endOut(int id) {
    return g.endOut(id);
  }

  @Override
  public final int getOutNode(long idx) {
    return g.getOutNode(idx);
  }

  @Override
  public long startIn(int id) {
    if (inIdxs == null)
      return 0;
    return id == 0 ? 0 : inIdxs[id - 1];
  }

  @Override
  public long endIn(int id) {
    if (inIdxs == null)
      return 0;
    return inIdxs[id];
  }

  @Override
  public int getInNode(long idx) {
    return ins[checkInt(idx)];
  }
}
