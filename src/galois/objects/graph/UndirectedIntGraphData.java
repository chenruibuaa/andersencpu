package galois.objects.graph;

import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.procedure.TIntIntProcedure;

import java.io.FileNotFoundException;
import java.io.IOException;

import util.fn.Lambda2;

class UndirectedIntGraphData extends AbstractEdgeData<Object> implements GraphData {
  private final int[] edgeData;
  private final int[] outIdxs;
  private final int[] outs;
  private final long numEdges;

  abstract static class CheckRev implements TIntIntProcedure {
    int id;
    int edgeIdx;
  }
  
  public UndirectedIntGraphData(GraphData g, EdgeData<Object> ed, final Lambda2<Integer,Integer,Integer> merge) throws FileNotFoundException, IOException {
    edgeData = new int[checkInt(g.getNumEdges())];
    outIdxs = new int[g.getNumNodes()];
    outs = new int[edgeData.length];
    
    CheckRev checkRev = new CheckRev() {
      @Override
      public boolean execute(int key, int value) {
        if (key < id) {
          long start = startOut(key);
          long end = endOut(key);
          for (long idx = start; idx < end; idx++) {
            int dst = getOutNode(idx);
            if (dst == id) {
              edgeData[checkInt(idx)] = merge.call(getInt(idx), value);
              return true;
            }
          }
        }
        
        outs[edgeIdx] = key;
        edgeData[edgeIdx++] = value;
        return true;
      }
    };
    
    TIntIntMap outEdges = new TIntIntHashMap();
    for (int id = 0; id < g.getNumNodes(); id++) {
      long start = g.startOut(id);
      long end = g.endOut(id);
      for (long idx = start; idx < end; idx++) {
        int dst = g.getOutNode(idx);
        int v = ed.getInt(idx);
        
        if (outEdges.containsKey(dst)) {
          outEdges.put(dst, merge.call(outEdges.get(dst), v));
        } else {
          outEdges.put(dst, v);
        }
      }
      checkRev.id = id;
      outEdges.forEachEntry(checkRev);
      outIdxs[id] = checkRev.edgeIdx;
      outEdges.clear();
    }
    
    numEdges = checkRev.edgeIdx;
  }
  
  @Override
  public int getInt(long idx) {
    return edgeData[checkInt(idx)];
  }

  @Override
  public Object get(long idx) {
    return getInt(idx);
  }

  private static int checkInt(long x) {
    assert x <= Integer.MAX_VALUE;
    assert x >= 0;
    return (int) x;
  }

  @Override
  public int getNumNodes() {
    return outIdxs.length;
  }

  @Override
  public long getNumEdges() {
    return numEdges;
  }

  @Override
  public long startOut(int id) {
    return id == 0 ? 0 : outIdxs[id - 1];
  }

  @Override
  public final long endOut(int id) {
    return outIdxs[id];
  }

  @Override
  public final int getOutNode(long idx) {
    return outs[checkInt(idx)];
  }

  @Override
  public long startIn(int id) {
    return 0;
  }

  @Override
  public long endIn(int id) {
    return 0;
  }

  @Override
  public int getInNode(long idx) {
    throw new UnsupportedOperationException();
  }
}
