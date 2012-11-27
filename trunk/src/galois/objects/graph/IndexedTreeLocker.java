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

File: IndexedTreeLocker.java 

 */

package galois.objects.graph;

import util.fn.Lambda0Void;
import galois.objects.GObject;
import galois.objects.MethodFlag;
import galois.runtime.GaloisRuntime;
import galois.runtime.Iteration;

class IndexedTreeLocker extends GraphLocker {

  static <N extends GObject> void removeNodeProlog(GNode<N> src, byte flags) {
    Iteration.access(src, flags);
  }

  static <N extends GObject> void getNeighborProlog(GNode<N> src, byte flags) {
    Iteration.access(src, flags);
  }

  static <N extends GObject> void getNeighborEpilog(GNode<N> dst, byte flags) {
    Iteration.access(dst, flags);
  }

  static <N extends GObject> void setNeighborProlog(GNode<N> src, GNode<N> dst, byte flags) {
    Iteration it = Iteration.access(src, flags);
    if (dst != null) {
      Iteration.access(it, dst, flags);
    }
  }

  static <N extends GObject> void setNeighborEpilog(GNode<N> old, byte flags) {
    if (old != null) {
      Iteration.access(old, flags);
    }
  }

  static <N extends GObject> void removeNeighborProlog(GNode<N> src, byte flags) {
    Iteration.access(src, flags);
  }

  static <N extends GObject> void removeNeighborEpilog(final IndexedGraph<N> graph, final GNode<N> src,
      final GNode<N> child, final int idx, byte flags) {
    if (child != null) {
      Iteration.access(child, flags);
    }
    if (GaloisRuntime.needMethodFlag(flags, MethodFlag.SAVE_UNDO)) {
      GaloisRuntime.getRuntime().onUndo(Iteration.getCurrentIteration(), new Lambda0Void() {
        @Override
        public void call() {
          graph.setNeighbor(src, child, idx, MethodFlag.NONE);
        }
      });
    }
  }
}
