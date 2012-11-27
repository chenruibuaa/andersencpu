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

 File: ConcurrentIntSparseBitVector.java
 */


package util.concurrent;

import gnu.trove.list.array.TIntArrayList;
import util.MutableBoolean;
import util.fn.Lambda2Void;
import util.fn.Lambda3Void;
import util.fn.Lambda4Void;
import util.fn.LambdaVoid;
import util.ints.IntSet;
import util.ints.IntSetIterator;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public final class ConcurrentIntSparseBitVector implements IntSet, Iterable<Integer> {

    private static final AtomicReferenceFieldUpdater<ConcurrentIntSparseBitVector, Element> headUpdater
            = AtomicReferenceFieldUpdater.newUpdater(ConcurrentIntSparseBitVector.class, Element.class, "head");

    // 64 bits per element
    private static final int MASK = 64 - 1;
    private static final byte LOG_BITS_PER_ELEMENT = 6;

    private volatile Element head;
    private final ThreadLocal<Element> cursor;

    public ConcurrentIntSparseBitVector() {
        head = null;
        cursor = new ThreadLocal<Element>();
        setCursor(head);
    }

    @Override
    public boolean add(final int n) {
        int index = n >> LOG_BITS_PER_ELEMENT;
        long bits = 1L << (n & MASK);
        return add_(index, bits);
    }

    private boolean add_(int index, long bits) {
        Element prev = find(getCursor(), head, index);
        for (; ; ) {
            if (prev == null) {
                final Element currFirst = head;
                if (currFirst == null || currFirst.index > index) {
                    final Element newElement = new Element(bits, index, currFirst);
                    if (headUpdater.compareAndSet(this, currFirst, newElement)) {
                        setCursor(newElement);
                        return true;
                    }
                }
                prev = findForward(head, index);
            } else if (prev.index == index) {
                setCursor(prev);
                return prev.or(bits);
            } else {
                final Element currPrevNext = prev.next;
                if (currPrevNext == null || currPrevNext.index > index) {
                    final Element newElement = new Element(bits, index, currPrevNext);
                    if (prev.casNext(currPrevNext, newElement)) {
                        setCursor(newElement);
                        return true;
                    }
                }
                prev = findForward(prev, index);
            }
        }
    }

    public boolean unionTo(final IntSet intSet) {
        ConcurrentIntSparseBitVector other = (ConcurrentIntSparseBitVector) intSet;
        boolean ret = false;
        Element otherCurr = other.head;
        while (otherCurr != null) {
            ret |= add_(otherCurr.index, otherCurr.bits);
            otherCurr = otherCurr.next;
        }
        return ret;
    }

    @NotThreadSafe
    public boolean serialUnionTo(final ConcurrentIntSparseBitVector other) {
        boolean ret = false;
        Element otherCurr = other.head;
        while (otherCurr != null) {
            ret |= serialAdd_(otherCurr.index, otherCurr.bits);
            otherCurr = otherCurr.next;
        }
        return ret;
    }

    @NotThreadSafe
    public boolean serialAdd(final int n) {
        int index = n >> LOG_BITS_PER_ELEMENT;
        long bits = 1L << (n & MASK);
        return add_(index, bits);
    }

    @NotThreadSafe
    private boolean serialAdd_(final int index, final long bits) {
        Element prev = find(getCursor(), head, index);
        if (prev == null) {
            head = new Element(bits, index, head);
            setCursor(head);
            return true;
        } else if (prev.index == index) {
            setCursor(prev);
            long prevBits = prev.bits;
            prev.bits |= bits;
            return prev.bits != prevBits;
        }
        Element element = new Element(bits, index, prev.next);
        setCursor(element);
        prev.next = element;
        return true;
    }

    private Element getCursor() {
        Element ret = cursor.get();
        assert isValid(ret);
        return ret;
    }

    private boolean isValid(final Element cursor) {
        Element curr = head;
        while (curr != null) {
            if (curr == cursor) {
                return true;
            }
            curr = curr.next;
        }
        return cursor == null;
    }

    private void setCursor(final Element newCursor) {
        cursor.set(newCursor);
    }

    @Override
    public boolean addAll(IntSet intSet) {
        final MutableBoolean ret = new MutableBoolean(false);
        intSet.map(new LambdaVoid<Integer>() {
            @Override
            public void call(Integer next) {
                boolean curr = ret.get();
                curr |= add(next);
                ret.set(curr);
            }
        });
        return ret.get();
    }

    @Override
    @NotThreadSafe
    public void clear() {
        head = null;
        setCursor(null);
    }

    @Override
    @NotThreadSafe
    public boolean contains(int n) {
        int index = n >> LOG_BITS_PER_ELEMENT;
        long bits = 1L << (n & MASK);
        Element element = findForward(head, index);
        return (element != null && element.index == index && ((element.bits & bits) != 0L));
    }

    @Override
    public boolean isEmpty() {
        return head == null;
    }

    @NotThreadSafe
    public boolean isSingleton() {
        return head != null && head.next == null && Long.bitCount(head.bits) == 1;
    }

    @Override
    @NotThreadSafe
    public int size() {
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
        ConcurrentIntSparseBitVector other = (ConcurrentIntSparseBitVector) o;
        Element elem = head;
        Element otherElem = other.head;
        while (elem != null) {
            if (otherElem == null || !elem.equals(otherElem)) {
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

    private static Element find(final Element curr, final Element first, final int index) {
        if (curr == null) {
            return null;
        }
        return curr.index > index ? findForward(first, index) : findForward(curr, index);
    }

    // returns the Element with index "index", or the previous one if it does not exist
    private static Element findForward(Element curr, final int index) {
        Element prev = null;
        while (curr != null) {
            int currIndex = curr.index;
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

    @Override
    public boolean remove(int x) {
        throw new UnsupportedOperationException();
    }

    @NotThreadSafe
    public void serialDiffTo(ConcurrentIntSparseBitVector other) {
        Element prevHead = null;
        Element thisHead = head;
        Element otherHead = other.head;
        while (thisHead != null && otherHead != null) {
            if (thisHead.index < otherHead.index) {
                prevHead = thisHead;
                thisHead = thisHead.next;
            } else if (thisHead.index < otherHead.index) {
                otherHead = otherHead.next;
            } else {
                // same index
                thisHead.bits &= (~otherHead.bits);
                if (thisHead.bits == 0) {
                    if (prevHead == null) {
                        // remove current head
                        head = thisHead.next;
                    } else {
                        prevHead.next = thisHead.next;
                    }
                } else {
                    prevHead = thisHead;
                }
                thisHead = thisHead.next;
                otherHead = otherHead.next;
            }
        }
    }

    @Override
    public Iterator<Integer> iterator() {
        return new SimpleIterator(head);
    }

    @Override
    public IntSetIterator intIterator() {
        return new SimpleIterator(head);
    }

    @Override
    public void map(LambdaVoid<Integer> fn) {
        if (isEmpty()) {
            return;
        }
        SimpleIterator iterator = new SimpleIterator(head);
        while (iterator.hasNext()) {
            fn.call(iterator.nextInt());
        }
    }

    @Override
    public <T1> void map(Lambda2Void<Integer, T1> fn, T1 arg1) {
        if (isEmpty()) {
            return;
        }
        SimpleIterator iterator = new SimpleIterator(head);
        while (iterator.hasNext()) {
            fn.call(iterator.nextInt(), arg1);
        }
    }

    @Override
    public <T1, T2> void map(Lambda3Void<Integer, T1, T2> fn, T1 arg1, T2 arg2) {
        if (isEmpty()) {
            return;
        }
        SimpleIterator iterator = new SimpleIterator(head);
        while (iterator.hasNext()) {
            fn.call(iterator.nextInt(), arg1, arg2);
        }
    }

    @Override
    public <T1, T2, T3> void map(Lambda4Void<Integer, T1, T2, T3> fn, T1 arg1, T2 arg2, T3 arg3) {
        if (isEmpty()) {
            return;
        }
        SimpleIterator iterator = new SimpleIterator(head);
        while (iterator.hasNext()) {
            fn.call(iterator.nextInt(), arg1, arg2, arg3);
        }
    }

    @Override
    public String toString() {
        final StringBuilder stringBuilder = new StringBuilder("[");
        map(new LambdaVoid<Integer>() {
            @Override
            public void call(Integer arg0) {
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

    @Override
    public ConcurrentIntSparseBitVector clone() {
        ConcurrentIntSparseBitVector ret = new ConcurrentIntSparseBitVector();
        if (head != null) {
            ret.head = head.clone();
        }
        return ret;
    }

    public TIntArrayList elements() {
        TIntArrayList ret = new TIntArrayList();
        if (!isEmpty()) {
            SimpleIterator iterator = new SimpleIterator(head);
            while (iterator.hasNext()) {
                ret.add(iterator.nextInt());
            }
        }
        return ret;
    }

    private static class Element {
        final int index;
        volatile long bits;
        volatile Element next;

        private static AtomicReferenceFieldUpdater<Element, Element> nextUpdater
                = AtomicReferenceFieldUpdater.newUpdater(Element.class, Element.class, "next");
        private static AtomicLongFieldUpdater<Element> bitsUpdater
                = AtomicLongFieldUpdater.newUpdater(Element.class, "bits");

        //copy constructor
        Element(long bits, int index, Element next) {
            this.bits = bits;
            this.index = index;
            this.next = next;
        }

        @Override
        public Element clone() {
            Element nextClone = next == null ? null : next.clone();
            return new Element(bits, index, nextClone);
        }

        boolean equals(Element other) {
            // non-recursive
            return other.index == index && other.bits == bits;
        }

        boolean or(long otherBits) {
            long prevBits, currBits;
            do {
                prevBits = bits;
                currBits = prevBits | otherBits;
                if (currBits == prevBits) {
                    // avoid CAS
                    return false;
                }
            } while (!bitsUpdater.compareAndSet(this, prevBits, currBits));
            return true;
        }

        boolean casNext(Element expected, Element next) {
            return nextUpdater.compareAndSet(this, expected, next);
        }
    }

    private static class SimpleIterator implements IntSetIterator {
        long bits;
        Element curr;

        final static byte[] LOG_BASE_2 = {-1, 0, 1, 39, 2, 15, 40, 23, 3, 12, 16, 59, 41, 19, 24, 54, 4, -1, 13, 10, 17,
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
        public Integer next() {
            return nextInt();
        }

        @Override
        public int nextInt() {
            long k = bits & (bits - 1);
            long diff = bits ^ k;
            bits = k;
            int result = curr.index << LOG_BITS_PER_ELEMENT;
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
