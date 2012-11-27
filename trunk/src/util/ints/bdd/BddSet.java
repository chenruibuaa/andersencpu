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

 File: BddSet.java
 */


package util.ints.bdd;

import gnu.trove.list.array.TIntArrayList;
import util.MutableBoolean;
import util.concurrent.NotThreadSafe;
import util.fn.Lambda2Void;
import util.fn.Lambda3Void;
import util.fn.Lambda4Void;
import util.fn.LambdaVoid;
import util.ints.IntSet;
import util.ints.IntSetIterator;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * BDD representation of a set.
 */
final public class BddSet implements IntSet {
    private static AtomicReferenceFieldUpdater<BddSet, BddNode> rootUpdater
            = AtomicReferenceFieldUpdater.newUpdater(BddSet.class, BddNode.class, "root");

    private volatile BddNode root;
    private static BddDomain domain;

    public BddSet() {
        root = Bdd.ZERO;
    }

    public BddSet(final BddNode root) {
        this.root = root;
    }

    public static void setDomain(BddDomain domain) {
        BddSet.domain = domain;
    }

    public boolean unionTo(final IntSet intSet) {
        // sorry, only bdd sets are allowed here.
        BddSet s = (BddSet) intSet;
        return unionTo(s.getRoot());
    }

    private boolean unionTo(final BddNode s) {
        BddNode currRoot, newRoot;
        do {
            currRoot = getRoot();
            newRoot = Bdd.or(currRoot, s);
            if (currRoot == newRoot) {
                return false;
            }
        } while (!rootUpdater.compareAndSet(this, currRoot, newRoot));
        return true;
    }

    @Override
    public boolean add(int number) {
        BddNode otherRoot = domain.getNodeVar(number);
        return unionTo(otherRoot);
    }

    @Override
    public boolean addAll(IntSet intSet) {
        final MutableBoolean ret = new MutableBoolean(false);
        map(new LambdaVoid<Integer>() {
            @Override
            public void call(Integer next) {
                boolean curr = ret.get();
                ret.set(add(next) || curr);
            }
        });
        return ret.get();
    }

    @Override
    public boolean remove(int n) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        rootUpdater.set(this, Bdd.ZERO);
    }

    @Override
    public boolean contains(int n) {
        BddNode root = getRoot();
        BddNode newRoot = Bdd.or(root, domain.getNodeVar(n));
        return newRoot == root;
    }

    public boolean intersectionTo(final BddSet s) {
        return intersectionTo(s.getRoot());
    }

    private boolean intersectionTo(final BddNode s) {
        BddNode currRoot, newRoot;
        do {
            currRoot = getRoot();
            newRoot = Bdd.and(currRoot, s);
            if (currRoot == newRoot) {
                return false;
            }
        } while (!rootUpdater.compareAndSet(this, currRoot, newRoot));
        return true;
    }

    @NotThreadSafe
    public boolean serialUnionTo(final BddSet s) {
        return serialUnionTo(s.getRoot());
    }

    @NotThreadSafe
    public boolean serialUnionTo(final BddNode s) {
        BddNode previousRoot = root;
        root = Bdd.or(previousRoot, s);
        return previousRoot != root;
    }

    @NotThreadSafe
    public boolean serialAdd(int number) {
        BddNode otherRoot = domain.getNodeVar(number);
        return serialUnionTo(otherRoot);
    }

    @NotThreadSafe
    public boolean serialRemove(int number) {
        BddNode otherRoot = domain.getNodeVar(number);
        BddNode currRoot = root;
        serialDiffTo(otherRoot);
        return currRoot != root;
    }

    @NotThreadSafe
    public BddSet serialIntersection(final BddSet s) {
        BddNode previousRoot = root;
        return new BddSet(Bdd.and(previousRoot, s.getRoot()));
    }

    @NotThreadSafe
    public void serialDiffTo(final BddNode s) {
        root = Bdd.diff(root, s);
    }

    @NotThreadSafe
    public void serialDiffTo(final BddSet s) {
        root = Bdd.diff(root, s.root);
    }

    @Override
    public void map(LambdaVoid<Integer> fn) {
        if (isEmpty()) {
            return;
        }
        TIntArrayList elements = elements();
        for (int i = 0; i < elements.size(); i++) {
            int index = elements.getQuick(i);
            fn.call(index);
        }
    }

    @Override
    public <A1> void map(Lambda2Void<Integer, A1> body, A1 arg1) {
        if (isEmpty()) {
            return;
        }
        TIntArrayList elements = elements();
        for (int i = 0; i < elements.size(); i++) {
            int index = elements.getQuick(i);
            body.call(index, arg1);
        }
    }

    @Override
    public <A1, A2> void map(Lambda3Void<Integer, A1, A2> body, A1 arg1, A2 arg2) {
        if (isEmpty()) {
            return;
        }
        TIntArrayList elements = elements();
        for (int i = 0; i < elements.size(); i++) {
            int index = elements.getQuick(i);
            body.call(index, arg1, arg2);
        }
    }

    @Override
    public <A1, A2, A3> void map(Lambda4Void<Integer, A1, A2, A3> body, A1 arg1, A2 arg2, A3 arg3) {
        if (isEmpty()) {
            return;
        }
        TIntArrayList elements = elements();
        for (int i = 0; i < elements.size(); i++) {
            int index = elements.getQuick(i);
            body.call(index, arg1, arg2, arg3);
        }
    }

    @Override
    public IntSetIterator intIterator() {
        throw new UnsupportedOperationException();
    }


    public TIntArrayList elements() {
        return domain.getElements(getRoot());
    }

    @Override
    public int size() {
        // avoid non-initialized domains
        if (isEmpty()) {
            return 0;
        }
        return domain.satCount(getRoot());
    }

    @Override
    public boolean isEmpty() {
        return root == Bdd.ZERO;
    }

    public BddNode getRoot() {
        return rootUpdater.get(this);
    }

    @Override
    public boolean equals(Object s) {
        return root == ((BddSet) s).root;
    }

    @Override
    public BddSet clone() {
        return new BddSet(rootUpdater.get(this));
    }

    @Override
    public int hashCode() {
        return root.hashCode();
    }

    @Override
    public String toString() {
        // avoid non-initialized domains
        if (isEmpty()) {
            return "[]";
        }
        return BddDomain.toString(domain, root);
    }
}
