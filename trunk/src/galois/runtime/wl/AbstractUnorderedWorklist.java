package galois.runtime.wl;

import galois.runtime.ThreadContext;

public abstract class AbstractUnorderedWorklist<T> implements UnorderedWorklist<T> {
  @Override
  public void add(T item, ThreadContext ctx) {
    throw new UnsupportedOperationException();
  }

  @Override
  public T poll(ThreadContext ctx) {
    throw new UnsupportedOperationException();
  }

  @Override
  public T polls() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addAborted(T item, ThreadContext ctx) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public int size() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Worklist<T> newInstance() {
    throw new UnsupportedOperationException();
  }
}
