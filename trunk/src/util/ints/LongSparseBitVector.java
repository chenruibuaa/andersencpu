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

 File: LongSparseBitVector.java
 */
package util.ints;

import util.MutableBoolean;
import util.fn.Lambda2Void;
import util.fn.Lambda3Void;
import util.fn.Lambda4Void;
import util.fn.LambdaVoid;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public final class LongSparseBitVector implements LongSet {

  private static AtomicIntegerFieldUpdater<LongSparseBitVector> sizeUpdater
      = AtomicIntegerFieldUpdater.newUpdater(LongSparseBitVector.class, "size");
  private static final int MASK = 63;
  private static final byte LOG_BITS_PER_ELEMENT = 6;

  private Element head, cursor;
  private volatile int size;
  private boolean changedSize;

  public LongSparseBitVector() {
    head = null;
    cursor = head;
    size = 0;
    changedSize = false;
  }

  public LongSparseBitVector(LongSparseBitVector other) {
    head = other.head == null ? null : other.head.clone();
    cursor = head;
    size = other.size();
    changedSize = false;
  }

  @Override
  public boolean add(final long bit) {
    long index = bit >> LOG_BITS_PER_ELEMENT;
    long bits = 1L << (bit & MASK);
    boolean ret = add_(index, bits);
    if (ret) {
      size++;
    }
    return ret;
  }

  public void unionTo(final LongSparseBitVector other) {
    Element otherCurr = other.head;
    while (otherCurr != null) {
      changedSize |= add_(otherCurr.index, otherCurr.bits);
      otherCurr = otherCurr.next;
    }
  }

  // might add many elements at once
  private boolean add_(final long index, final long bits) {
    Element prev = find(cursor, head, index);
    if (prev == null) {
      head = new Element(bits, index, head);
      cursor = head;
      return true;
    } else if (prev.index == index) {
      cursor = prev;
      long prevBits = prev.bits;
      prev.bits |= bits;
      return prev.bits != prevBits;
    }
    Element element = new Element(bits, index, prev.next);
    cursor = element;
    prev.next = element;
    return true;
  }

  public boolean concurrentAdd(final int n) {
    int index = n >> LOG_BITS_PER_ELEMENT;
    long bits = 1L << (n & MASK);
    boolean ret = concurrentAdd_(index, bits);
    if (ret) {
      sizeUpdater.getAndIncrement(this);
    }
    return ret;
  }

  private boolean concurrentAdd_(final int index, final long bits) {
    Element prev = findForward(head, index);
    for (; ;) {
      if (prev == null) {
        synchronized (this) {
          if (head == null || head.index > index) {
            head = new Element(bits, index, head);
            cursor = head;
            return true;
          }
        }
        prev = findForward(head, index);
      } else if (prev.index == index) {
        cursor = prev;
        return prev.or(bits);
      } else {
        Element tmp = new Element(bits, index, null);
        synchronized (prev) {
          Element prevNext = prev.next;
          if (prevNext == null || prevNext.index > index) {
            tmp.next = prevNext;
            prev.next = tmp;
            cursor = tmp;
            return true;
          }
        }
        prev = findForward(prev, index);
      }
    }
  }

  @Override
  public boolean addAll(LongSet intSet) {
    final MutableBoolean ret = new MutableBoolean(false);
    map(new LambdaVoid<Long>() {
      @Override
      public void call(Long next) {
        boolean curr = ret.get();
        ret.set(add(next) || curr);
      }
    });
    return ret.get();
  }

  @Override
  public boolean remove(long n) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    head = null;
    cursor = null;
    size = 0;
    changedSize = false;
  }

  @Override
  public boolean contains(long bit) {
    long index = bit >> LOG_BITS_PER_ELEMENT;
    long pos = bit & MASK;
    Element prev = findForward(head, index);
    if (prev == null || prev.index != index) {
      return false;
    } else {
      long mask = 1L << pos;
      return (prev.bits & mask) != 0L;
    }
  }

  @Override
  public boolean isEmpty() {
    return head == null;
  }

  public boolean isSingleton() {
    return size() == 1;
  }

  @Override
  public int size() {
    if (changedSize) {
      size = computeSize();
      changedSize = false;
    }
    return size;
  }

  private int computeSize() {
    int ret = 0;
    Element elem = head;
    while (elem != null) {
      ret += Long.bitCount(elem.bits);
      elem = elem.next;
    }
    return ret;
  }

  @Override
  public boolean equals(Object o) {
    LongSparseBitVector other = (LongSparseBitVector) o;
    if (size() != other.size()) {
      return false;
    }
    Element elem = head;
    Element otherElem = other.head;
    while (elem != null) {
      // the equality should rely on Element.equals, we inline it here to avoid casts
      if (otherElem == null || elem.index != otherElem.index || elem.bits != otherElem.bits) {
        return false;
      }
      elem = elem.next;
      otherElem = otherElem.next;
    }
    return otherElem == null;
  }

  @Override
  public int hashCode() {
    long ret = 0;
    Element elem = head;
    while (elem != null) {
      ret ^= elem.index;
      ret ^= elem.bits;
      elem = elem.next;
    }
    return (int) (ret);
  }


  private static Element find(final Element curr, final Element first, final long index) {
    if (curr == null) {
      return null;
    }
    return curr.index > index ? findForward(first, index) : findForward(curr, index);
  }

  // returns the Element with index "index", or the previous one if it does not exist
  private static Element findForward(Element curr, final long index) {
    Element prev = null;
    while (curr != null) {
      long currIndex = curr.index;
      if (currIndex < index) {
        prev = curr;
        curr = curr.next;
      } else if (currIndex > index) {
        return prev;
      } else {
        return curr;
      }
    }
    return prev;
  }

  public void map(LambdaVoid<Long> fn) {
    if (isEmpty()) {
      return;
    }
    SimpleIterator iterator = new SimpleIterator(head);
    while (iterator.hasNext()) {
      fn.call(iterator.nextLong());
    }
  }

  @Override
  public <T1> void map(Lambda2Void<Long, T1> fn, T1 arg1) {
    if (isEmpty()) {
      return;
    }
    SimpleIterator iterator = new SimpleIterator(head);
    while (iterator.hasNext()) {
      fn.call(iterator.nextLong(), arg1);
    }
  }

  @Override
  public <T1, T2> void map(Lambda3Void<Long, T1, T2> fn, T1 arg1, T2 arg2) {
    if (isEmpty()) {
      return;
    }
    SimpleIterator iterator = new SimpleIterator(head);
    while (iterator.hasNext()) {
      fn.call(iterator.nextLong(), arg1, arg2);
    }
  }

  @Override
  public <T1, T2, T3> void map(Lambda4Void<Long, T1, T2, T3> fn, T1 arg1, T2 arg2, T3 arg3) {
    if (isEmpty()) {
      return;
    }
    SimpleIterator iterator = new SimpleIterator(head);
    while (iterator.hasNext()) {
      fn.call(iterator.nextLong(), arg1, arg2, arg3);
    }
  }

  @Override
  public String toString() {
    final StringBuilder stringBuilder = new StringBuilder("[");
    map(new LambdaVoid<Long>() {
      @Override
      public void call(Long arg0) {
        stringBuilder.append(arg0).append(", ");
      }
    });
    int length = stringBuilder.length();
    if (length > 1) {
      stringBuilder.deleteCharAt(length - 1);
      stringBuilder.deleteCharAt(length - 2);
    }
    stringBuilder.append("]");
    return stringBuilder.toString();
  }

  private static class Element {
    final long index;
    long bits;
    Element next;

    Element(long bits, long index, Element next) {
      this.index = index;
      this.bits = bits;
      this.next = next;
    }

    private boolean or(final long otherBits) {
      // slow path
      synchronized (this) {
        long prevBits = bits;
        bits |= otherBits;
        return bits != prevBits;
      }
    }

    @Override
    protected Element clone() {
      Element ret = new Element(bits, index, null);
      if (next != null) {
        ret.next = next.clone();
      }
      return ret;
    }
  }

  private static class SimpleIterator implements LongSetIterator {
    long bits;
    Element curr;

    final static int[] LOG_BASE_2 = {-1, 0, 1, 39, 2, 15, 40, 23, 3, 12, 16, 59, 41, 19, 24, 54, 4, -1, 13, 10, 17,
        62, 60, 28, 42, 30, 20, 51, 25, 44, 55, 47, 5, 32, -1, 38, 14, 22, 11, 58, 18, 53, -1, 9, 61, 27, 29, 50, 43,
        46, 31, 37, 21, 57, 52, 8, 26, 49, 45, 36, 56, 7, 48, 35, 6, 34, 33};

    private SimpleIterator(Element first) {
      curr = first;
      if (curr != null) {
        bits = curr.bits;
      }
    }

    @Override
    public boolean hasNext() {
      return curr != null;
    }

    @Override
    public Long next() {
      return nextLong();
    }

    @Override
    public long nextLong() {
      long k = bits & (bits - 1);
      long diff = bits ^ k;
      bits = k;
      long result = curr.index << LOG_BITS_PER_ELEMENT;
      result += (diff < 0) ? 63 : LOG_BASE_2[(int) (diff % 67)];
      if (bits == 0) {
        curr = curr.next;
        if (curr != null) {
          bits = curr.bits;
        }
      }
      return result;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
