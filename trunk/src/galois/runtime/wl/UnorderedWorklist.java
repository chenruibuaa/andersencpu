package galois.runtime.wl;

import galois.runtime.ThreadContext;


public interface UnorderedWorklist<T> extends Worklist<T> {
  public void addAborted(T item, ThreadContext ctx);
}
