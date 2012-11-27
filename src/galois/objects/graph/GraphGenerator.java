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

File: GraphGenerator.java 

 */

package galois.objects.graph;

import galois.objects.GObject;
import galois.objects.Mappable;
import galois.objects.Mappables;
import galois.runtime.ForeachContext;
import galois.runtime.GaloisRuntime;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import util.Launcher;
import util.Pair;
import util.TokenizingReader;
import util.concurrent.AtomicBitArray;
import util.fn.Lambda;
import util.fn.LambdaVoid;

// TODO(ddn): refactor into ReaderGraph
class GraphGenerator {
  private static final String PROBLEM = "p";
  private static final String ARC = "a";
  private static final String COMMENT = "c";

  private static final String FILE = "file";
  private static final String GRID = "grid";
  private static final String RAND = "rand";

  private static final int MAX_EDGE_WEIGHT = 1000;

  private static <N extends GObject> List<GNode<N>> createNodes(Graph<N> graph, int numNodes,
      Lambda<Integer, N> nodeDataGenerator) {
    List<GNode<N>> nodes = new ArrayList<GNode<N>>();
    for (int i = 0; i < numNodes; ++i) {
      N nodeData = nodeDataGenerator.call(i);
      GNode<N> gn = graph.createNode(nodeData);
      if (graph.add(gn)) {
        nodes.add(gn);
      }
    }
    return nodes;
  }

  /**
   * 
   * @param rows
   * @param cols
   * @return a 2D mesh with random edge weights
   */
  private static <N extends GObject, E> void genGrid(ObjectGraph<N, E> graph, Lambda<Integer, N> nodeDataGen,
      Lambda<ForeachContext<?>, E> edgeDataGen, int rows, int cols) {

    List<GNode<N>> nodes = createNodes(graph, rows * cols, nodeDataGen);

    for (int i = 0; i < rows; ++i) {
      for (int j = 0; j < cols; ++j) {
        List<GNode<N>> neighbors = new ArrayList<GNode<N>>();

        int index = getIndex(i + 1, j, rows, cols); // East
        if (index >= 0) {
          neighbors.add(nodes.get(index));
        }

        index = getIndex(i - 1, j, rows, cols); // West
        if (index >= 0) {
          neighbors.add(nodes.get(index));
        }

        index = getIndex(i, j + 1, rows, cols); // North
        if (index >= 0) {
          neighbors.add(nodes.get(index));
        }

        index = getIndex(i, j - 1, rows, cols); // South
        if (index >= 0) {
          neighbors.add(nodes.get(index));
        }

        GNode<N> node = nodes.get(getIndex(i, j, rows, cols));
        for (GNode<N> m : neighbors) {
          E ed = edgeDataGen.call(null);
          graph.addEdge(node, m, ed);
        }
      }
    }
  }

  public static <N extends GObject> void readIntegerEdgeGraph(String grPath, ObjectGraph<N, Integer> emptyGraph,
      Lambda<Integer, N> nodeDataGen) throws IOException {

    parseGrFile(emptyGraph, nodeDataGen, grPath);
  }

  public static <N extends GObject> String[] readIntegerEdgeGraph(String[] args, ObjectGraph<N, Integer> emptyGraph,
      Lambda<Integer, N> nodeDataGen) throws IOException {

    final Random rand = new Random(MAX_EDGE_WEIGHT);
    Lambda<ForeachContext<?>, Integer> edgeDataGen = new Lambda<ForeachContext<?>, Integer>() {
      @Override
      public Integer call(ForeachContext<?> ctx) {
        return rand.nextInt(MAX_EDGE_WEIGHT);
      }
    };

    String type = args[0];
    int start = 0;
    if (type.equals(RAND)) {
      int numNodes = Integer.parseInt(args[1]);
      int minDegree = Integer.parseInt(args[2]);
      int maxDegree = Integer.parseInt(args[3]);

      randGraph(emptyGraph, nodeDataGen, edgeDataGen, numNodes, minDegree, maxDegree);
      start = 4;
    } else if (type.equals(GRID)) {
      int rows = Integer.parseInt(args[1]);
      int cols = Integer.parseInt(args[2]);
      genGrid(emptyGraph, nodeDataGen, edgeDataGen, rows, cols);
      start = 3;
    } else if (type.equals(FILE)) {
      String grPath = args[1];
      parseGrFile(emptyGraph, nodeDataGen, grPath);
      start = 2;
    } else {
      usage();
    }

    int length = args.length - start;
    String[] retval = new String[length];
    System.arraycopy(args, start, retval, 0, length);
    return retval;
  }

  private static int getIndex(int i, int j, int rows, int cols) {
    if (i < 0 || i >= rows || j < 0 || j >= cols) {
      return -1;
    }

    return cols * i + j;
  }

  /**
   * 
   * @param numNodes
   * @return a Random instance seeded with numNodes, that way for a given input
   *         config, multiple runs will result in similar behavior, but the
   *         behavior will vary when numNodes is changed.
   */
  private static Random getRandom(int numNodes) {
    return new Random(numNodes);
  }

  /**
   * .gr file format
   * 
   * <pre>
   * c 9th DIMACS Implementation Challenge: Shortest Paths
   * c http://www.dis.uniroma1.it/~challenge9
   * c Sample graph file 
   * c
   * p sp 6 8
   * c graph contains 6 nodes and 8 arcs c node ids are numbers in 1..6
   * c
   * a 1 2 17
   * c arc from node 1 to node 2 of weight 17
   * c
   * a 1 3 10
   * a 2 4 2
   * a 3 5 0
   * a 4 3 0
   * a 4 6 3
   * a 5 2 0
   * a 5 6 20
   * </pre>
   * 
   * @throws ExecutionException
   */
  private static <N extends GObject> void parseGrFile(ObjectGraph<N, Integer> graph, Lambda<Integer, N> nodeDataGen,
      String filename) throws IOException {
    if (filename.endsWith(".chgr.gz")) {
      parseChunkedGrFile(graph, nodeDataGen, filename);
    } else {
      readGrLines(filename, graph, readFirstGrFile(graph, nodeDataGen, filename));
    }
  }

  private static Pair<Integer, String> parseChunkedGrFilename(String filename) {
    Pattern p = Pattern.compile("1-of-(\\d+)");
    Matcher m = p.matcher(filename);
    if (m.find()) {
      int max = Integer.parseInt(m.group(1));
      StringBuffer sb = new StringBuffer();
      m.appendReplacement(sb, "%d-of-" + max);
      m.appendTail(sb);
      return new Pair<Integer, String>(max, sb.toString());
    } else {
      throw new Error("unknown file type: " + filename);
    }
  }

  private static <N extends GObject> void parseChunkedGrFile(final ObjectGraph<N, Integer> graph,
      Lambda<Integer, N> nodeDataGen, String filename) throws IOException {
    final List<GNode<N>> nodes = readFirstGrFile(graph, nodeDataGen, filename);
    Pair<Integer, String> result = parseChunkedGrFilename(filename);
    int max = result.getFirst();
    final String base = result.getSecond();

    Mappable<Pair<Integer, String>> product = Mappables.product(Mappables.range(1, max + 1, 1),
        new Lambda<Integer, Mappable<String>>() {
          @Override
          public Mappable<String> call(Integer index) {
            String filename = String.format(base, index);
            try {
              return Mappables.fromReader(readFile(filename));
            } catch (FileNotFoundException e) {
              throw new Error(e);
            } catch (IOException e) {
              throw new Error(e);
            }
          }
        });

    final Pattern spliter = Pattern.compile("[ \t]+");

    try {
      // XXX(ddn): Hack to get around the fact that we don't detect conflicts in
      // forall loops
      final AtomicBitArray locks = new AtomicBitArray(nodes.size());

      GaloisRuntime.forall(product, new LambdaVoid<Pair<Integer, String>>() {
        @Override
        public void call(Pair<Integer, String> pair) {
          String line = pair.getSecond();

          if (line.startsWith(ARC)) {
            String[] tokens = spliter.split(line);

            if (tokens.length == 4) {
              // the node numbers in the file start from 1
              int sid = Integer.parseInt(tokens[1]) - 1;
              int did = Integer.parseInt(tokens[2]) - 1;
              GNode<N> src = nodes.get(sid);
              GNode<N> dst = nodes.get(did);
              int weight = Integer.parseInt(tokens[3]);

              boolean done = false;
              while (!done) {
                if (locks.compareAndSet(sid, false, true)) {
                  if (locks.compareAndSet(did, false, true)) {
                    if (!graph.addEdge(src, dst, weight)) {
                      // Edge already in graph
                      graph.setEdgeData(src, dst, Math.min(weight, graph.getEdgeData(src, dst)));
                    }
                    done = true;
                    do {
                    } while (!locks.compareAndSet(did, true, false));
                  }
                  do {
                  } while (!locks.compareAndSet(sid, true, false));
                }
              }
              return;
            }
          } else if (line.startsWith(COMMENT)) {
            return;
          } else if (line.startsWith(PROBLEM)) {
            return;
          }

          int index = pair.getFirst();
          String filename = String.format(base, index);
          throw new Error(String.format("unknown line in file %s", filename));
        }
      });
    } catch (ExecutionException e) {
      throw new IOException(e);
    }
  }

  private static <N extends GObject> List<GNode<N>> readFirstGrFile(ObjectGraph<N, Integer> graph,
      Lambda<Integer, N> nodeDataGen, String filename) throws IOException {
    TokenizingReader r = readFile(filename);
    try {
      TokenizingReader.TokenType t;
      while ((t = r.readNextToken()) == TokenizingReader.TokenType.STRING) {
        char c = r.getString().charAt(0);

        if (c == 'a') {
          ;
        } else if (c == 'c') {
          ;
        } else if (c == 'p') {
          r.readString(); // "sp"
          int numNodes = r.readInteger();
          return createNodes(graph, numNodes, nodeDataGen);
        } else {
          throw new Error(
              String.format("unknown character '%c' in file %s line %d", c, filename, r.getLineNumber() + 1));
        }
        r.readUntilEnd();
      }
      System.out.println(t);
      throw new Error("could not find the problem line in file " + filename);
    } finally {
      r.close();
    }
  }

  private static TokenizingReader readFile(String filename) throws FileNotFoundException, IOException {
    return new TokenizingReader(new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(
        filename)))), 1024);
  }

  private static <N extends GObject> void readGrLines(String filename, ObjectGraph<N, Integer> graph,
      List<GNode<N>> nodes) throws IOException {
    TokenizingReader r = readFile(filename);
    try {
      while (r.readNextToken() == TokenizingReader.TokenType.STRING) {
        char c = r.getString().charAt(0);

        if (c == 'a') {
          GNode<N> src = nodes.get(r.readInteger() - 1);
          GNode<N> dst = nodes.get(r.readInteger() - 1);
          int weight = r.readInteger();
          if (!graph.addEdge(src, dst, weight)) {
            graph.setEdgeData(src, dst, Math.min(weight, graph.getEdgeData(src, dst)));
          }
        } else if (c == 'c') {
          ;
        } else if (c == 'p') {
          ;
        } else {
          throw new Error(String.format("unknown character '%c' in file %s line %d %c", c, filename,
              r.getLineNumber() + 1));
        }
        r.readUntilEnd();
      }
    } finally {
      r.close();
    }
  }

  private static <N extends GObject, E> void randGraph(ObjectGraph<N, E> graph, Lambda<Integer, N> nodeDataGen,
      Lambda<ForeachContext<?>, E> edgeDataGenerator, int numNodes, int minDegree, int maxDegree) {

    assert minDegree <= maxDegree;

    List<GNode<N>> nodes = createNodes(graph, numNodes, nodeDataGen);

    Random rand = getRandom(numNodes);

    int deltaMax = maxDegree - minDegree;
    for (GNode<N> node : nodes) {
      int degree = (deltaMax <= 0) ? minDegree : minDegree + rand.nextInt(deltaMax);

      int i = graph.outNeighborsSize(node);
      while (i < degree) {
        GNode<N> m = nodes.get(rand.nextInt(numNodes)); // select a random node

        if (node == m || graph.hasNeighbor(node, m)) {
          continue;
        }

        E edgeData = edgeDataGenerator.call(null);
        graph.addEdge(node, m, edgeData);

        ++i;
      }
    }
  }

  private static void usage() {
    System.err.println("Provide as Arguments:");
    System.err.println("  rand <num-nodes> <min-neighbors> <max-neighbors>");
    System.err.println("| grid <rows> <cols>");
    System.err.println("| file <filename>");
    throw new Error();
  }
}
