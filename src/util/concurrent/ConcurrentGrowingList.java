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

 File: ConcurrentGrowingList.java
 */
package util.concurrent;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.atomic.AtomicInteger;

// Does NOT accept nulls
public class ConcurrentGrowingList<T> implements List<T> {

  private final Object[] list;
  private final AtomicInteger size;

  public ConcurrentGrowingList(int maxSize) {
    list = new Object[maxSize];
    size = new AtomicInteger(0);
  }

  @Override
  public int size() {
    return size.get();
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public boolean contains(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterator<T> iterator() {
    return new SimpleIterator();
  }

  @Override
  public Object[] toArray() {
    throw new UnsupportedOperationException();
  }

  @Override
  public <B> B[] toArray(B[] ts) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean add(T t) {
    int pos = size.getAndIncrement();
    list[pos] = t;
    return true;
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsAll(Collection<?> objects) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addAll(Collection<? extends T> ts) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addAll(int i, Collection<? extends T> ts) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeAll(Collection<?> objects) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean retainAll(Collection<?> objects) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    size.set(0);
  }

  @SuppressWarnings("unchecked")
  @Override
  public T get(int i) {
    return (T) list[i];
  }

  @Override
  public T set(int i, T t) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void add(int i, T t) {
    throw new UnsupportedOperationException();
  }

  @Override
  public T remove(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int indexOf(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int lastIndexOf(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListIterator<T> listIterator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListIterator<T> listIterator(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<T> subList(int i, int i1) {
    throw new UnsupportedOperationException();
  }

  private class SimpleIterator implements Iterator<T> {
    int curr = 0;

    @Override
    public boolean hasNext() {
      return curr < size.get();
    }

    @SuppressWarnings("unchecked")
    @Override
    public T next() {
      return (T) list[curr++];
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
