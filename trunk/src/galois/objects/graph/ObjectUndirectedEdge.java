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

File: ObjectUndirectedEdge.java 

 */

package galois.objects.graph;

import galois.objects.GObject;
import galois.runtime.Iteration;

public class ObjectUndirectedEdge<N extends GObject, E> implements GObject {
  private final E data;
  private GNode<N> src;
  private GNode<N> dst;

  public ObjectUndirectedEdge(GNode<N> src, GNode<N> dst, E data) {
    this.src = src;
    this.dst = dst;
    this.data = data;
  }

  public final GNode<N> getSrc() {
    return src;
  }

  public final GNode<N> getDst() {
    return dst;
  }

  public final E getData() {
    return data;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public boolean equals(Object obj) {
    if (obj == null)
      return false;

    if (!(obj instanceof ObjectUndirectedEdge))
      return false;

    ObjectUndirectedEdge o = (ObjectUndirectedEdge) obj;

    boolean e = (src == o.src && dst == o.dst) || (dst == o.src && src == o.dst);
    return e && (data == null || data.equals(o.data));
  }

  @Override
  public int hashCode() {
    int ret = super.hashCode();
    return ret * 31 + data.hashCode();
  }

  @Override
  public void access(Iteration it, byte flags) {
    Iteration.access(it, src, flags);
    Iteration.access(it, dst, flags);
  }
}
