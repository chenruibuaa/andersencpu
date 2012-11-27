package galois.objects.graph;

import galois.objects.GObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import util.fn.Lambda2;

public class ReaderGraphs {

  private static class Builder {
    protected String filename;
    protected boolean withInEdges;

    /**
     * Specifies file specification of output graph.
     * 
     * @param filename
     * @return
     */
    protected Builder from(String filename) {
      this.filename = filename;
      return this;
    }

    /**
     * Specifies if output graph should have in edges.
     * 
     * @param hasInEdges
     * @return
     */
    protected Builder withInEdges(boolean hasInEdges) {
      this.withInEdges = hasInEdges;
      return this;
    }
  }

  public static class GraphBuilder extends Builder {
    private boolean undirected;
    
    @Override
    public GraphBuilder from(String filename) {
      super.from(filename);
      return this;
    }

    @Override
    public GraphBuilder withInEdges(boolean withInEdges) {
      super.withInEdges(withInEdges);
      return this;
    }
    
    public GraphBuilder undirected(boolean undirected) {
      this.undirected = undirected;
      return this;
    }
    
    public Graph<GObject> create() throws FileNotFoundException, IOException {
      // TODO(ddn): Too lazy to remove duplicate edges, but algorithms should be robust to this case
      MappedFileGraphData data = new MappedFileGraphData(new File(filename), MappedFileGraphData.EdgeType.VOID);
      GraphData g = withInEdges ? new WithInEdgesGraphData(data) : data;
      return new ReaderGraph<GObject>(g, data.getEdgeData(), undirected);
    }
  }

  public static class IntGraphBuilder extends Builder {
    private Lambda2<Integer,Integer,Integer> merge;
    
    public final static Lambda2<Integer,Integer,Integer> MIN = new Lambda2<Integer,Integer,Integer>() {
      @Override
      public Integer call(Integer arg0, Integer arg1) {
        return arg0 < arg1 ? arg0 : arg1;
      }
    };
    
    @Override
    public IntGraphBuilder from(String filename) {
      super.from(filename);
      return this;
    }

    @Override
    public IntGraphBuilder withInEdges(boolean withInEdges) {
      super.withInEdges(withInEdges);
      return this;
    }

    public IntGraphBuilder undirected(Lambda2<Integer,Integer,Integer> merge) {
      this.merge = merge;
      return this;
    }
    
    public IntGraph<GObject> create() throws FileNotFoundException, IOException {
      MappedFileGraphData data = new MappedFileGraphData(new File(filename), MappedFileGraphData.EdgeType.INT);
      GraphData g;
      EdgeData<Object> ed;
      boolean undirected;
      if (merge == null) {
        g = withInEdges ? new WithInEdgesGraphData(data) : data;
        ed = data.getEdgeData();
        undirected = false;
      } else {
        UndirectedIntGraphData d = new UndirectedIntGraphData(data, data.getEdgeData(), merge);
        g = new WithInEdgesGraphData(d);
        ed = d;
        undirected = true;
      }
      return new ReaderGraph<GObject>(g, ed, undirected);
    }
  }
}
