package galois.runtime.wl;

import galois.runtime.ThreadContext;

import java.util.Comparator;

public class OrderedWorklistImpl<T> implements OrderedWorklist<T> {
  private final OrderedWorklist<T> wl;
  
  public OrderedWorklistImpl(OrderedWorklist<T> wl) {
    this.wl = wl;
  }

  @Override
  public void addAborted(T item, ThreadContext ctx) {
    wl.add(item, ctx);
  }

  @Override
  public void add(T item, ThreadContext ctx) {
    wl.add(item, ctx);
  }

  @Override
  public T poll(ThreadContext ctx) {
    return wl.poll(ctx);
  }

  @Override
  public T polls() {
    return wl.polls();
  }

  @Override
  public int size() {
    return wl.size();
  }

  @Override
  public Worklist<T> newInstance() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Comparator<T> getComparator() {
    return wl.getComparator();
  }

  @Override
  public T peek() {
    return wl.peek();
  }
}
