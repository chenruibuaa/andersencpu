package galois.objects.graph;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Represents graphs from files.
 * <p>
 * The file format is the following (all values are little endian):
 * </p>
 * 
 * <pre>
 * uint64_t version // should be 1 
 * uint64_t edgeDataSize 
 * uint64_t numNodes
 * uint64_t numEdges
 * uint64_t[numNodes] outIdxs // outIdxs[n] is the first edge for node n + 1, node 0 has implicit start indexof 0
 * uint32_t[numEdges] outs
 * uint32_t padding // to realign to 64-bits
 * edgeDataSize[numEdges] outData
 * </pre>
 */
class MappedFileGraphData implements GraphData {
  private final LongBuffer outIdxs;
  private final IntBuffer outs;
  private final MappedByteBuffer buf;
  private final EdgeType edgeType;
  private final int numNodes;
  private final long numEdges;

  public MappedFileGraphData(File in, EdgeType edgeType) throws FileNotFoundException, IOException {
    this.edgeType = edgeType;
    buf = new FileInputStream(in).getChannel().map(FileChannel.MapMode.READ_ONLY, 0, in.length());
    buf.order(ByteOrder.LITTLE_ENDIAN);

    long version = checkLong(buf.getLong());
    assert version == 1;

    long size = checkLong(buf.getLong());
    assert size == edgeType.size() || edgeType.size() == 0;

    numNodes = checkInt(buf.getLong());
    numEdges = checkLong(buf.getLong());

    outIdxs = buf.slice().order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
    buf.position(buf.position() + checkInt(numNodes) * 8);
    outs = buf.slice().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
    buf.position(buf.position() + checkInt(numEdges) * 4);

    // Need padding
    if ((numEdges & 1) == 1)
      buf.getInt();
  }

  public <E> EdgeData<E> getEdgeData() {
    final MappedByteBuffer mybuf = buf;

    switch (edgeType) {
    case VOID:
      return new AbstractEdgeData<E>() {
        @Override
        public Object get(long idx) {
          return null;
        }
      };
    case INT:
      return new AbstractEdgeData<E>() {
        IntBuffer data = mybuf.slice().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();

        @Override
        public int getInt(long idx) {
          return data.get(checkInt(idx));
        }

        @Override
        public Object get(long idx) {
          return getInt(idx);
        }
      };
    case FLOAT:
      return new AbstractEdgeData<E>() {
        FloatBuffer data = mybuf.slice().order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer();

        @Override
        public float getFloat(long idx) {
          return data.get(checkInt(idx));
        }

        @Override
        public Object get(long idx) {
          return getFloat(idx);
        }
      };
    }
    throw new Error();
  }

  private static long checkLong(long x) {
    assert x >= 0 : "overflowed signed long";
    return x;
  }

  private static int checkInt(long x) {
    assert x <= Integer.MAX_VALUE;
    assert x >= 0;
    return (int) x;
  }

  @Override
  public int getNumNodes() {
    return numNodes;
  }

  @Override
  public long getNumEdges() {
    return numEdges;
  }

  @Override
  public long startOut(int id) {
    return id == 0 ? 0 : outIdxs.get(id - 1);
  }

  @Override
  public final long endOut(int id) {
    return outIdxs.get(id);
  }

  @Override
  public final int getOutNode(long idx) {
    return outs.get(checkInt(idx));
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

  public enum EdgeType {
    VOID(0), INT(4), FLOAT(4), DOUBLE(8), LONG(8);
    int size;

    EdgeType(int size) {
      this.size = size;
    }

    public int size() {
      return size;
    }
  }
}
