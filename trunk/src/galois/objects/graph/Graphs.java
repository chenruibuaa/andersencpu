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

File: Graphs.java 

 */

package galois.objects.graph;

import galois.objects.GObject;

import java.util.Random;

/**
 * This class contains static utility methods that operate on or return objects of type {@link Graph}
 */
public class Graphs {

  /**
   * Retrieves a random node from the graph.
   * @param graph the graph to choose the node from
   * @param <N> the type of the data contained in a node
   * @return a random vertex contained in the indicated graph
   */
  public static <N extends GObject> GNode<N> getRandom(Graph<N> graph) {
    return getRandom(graph, new Random());
  }

  /**
   * Retrieves a random node from the graph.
   * @param graph the graph to choose the node from
   * @param seed a seed used to initialize the random generator
   * @param <N> the type of the data contained in a node
   * @return a random vertex contained in the indicated graph
   */
  public static <N extends GObject> GNode<N> getRandom(Graph<N> graph, Random random) {
    int size = graph.size();
    int randomId = random.nextInt(size);
    int id = 0;
    for (GNode<N> n : graph) {
      if (id++ == randomId) {
        return n;
      }
    }
    throw new Error();
  }
}
