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

 File: OnlineNode.java
 */

package hardekopfPointsTo.main;

import galois.objects.MethodFlag;
import galois.runtime.Iteration;
import util.concurrent.ConcurrentIntSparseBitVector;
import util.concurrent.ConcurrentLongSparseBitVector;
import util.concurrent.NotThreadSafe;
import util.fn.Lambda2Void;
import util.fn.Lambda3Void;
import util.fn.Lambda4Void;
import util.fn.LambdaVoid;
import util.ints.IntSet;
import util.ints.bdd.BddSet;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public final class OnlineNode implements Node {

    private static final AtomicReferenceFieldUpdater<OnlineNode, BddSet> prevPointsToUpdater = AtomicReferenceFieldUpdater
            .newUpdater(OnlineNode.class, BddSet.class, "prevPointsTo");

    private static final AtomicIntegerFieldUpdater<OnlineNode> repUpdater = AtomicIntegerFieldUpdater.newUpdater(
            OnlineNode.class, "representative");

    private static final AtomicIntegerFieldUpdater<OnlineNode> inWorklistUpdater = AtomicIntegerFieldUpdater.newUpdater(
            OnlineNode.class, "inWorklist");

    static int lastObjectNode;

    // Special node IDs: 0 - no node
    // unknown target of pointers cast from int
    final static int I2P = 1;
    // constant ptr to i2p,
    final static int P_I2P = 2;
    // the 1st node representing a real variable.
    final static int FIRST_VAR_NODE = 3;

    private static final int NODE_RANK_MIN = 90000000;

    private static final int NON_POINTER_MASK = 1 << 31;

    private static final int FUNCTION_MASK = 1 << 30;

    private volatile int representative;

    volatile int inWorklist;

    //final AtomicReference<Iteration> owner;

    final int id;

    private long flagsAndSize;

    final ConcurrentIntSparseBitVector load, store, copy;

    final ConcurrentLongSparseBitVector gep;

    private volatile BddSet prevPointsTo;
    final BddSet pointsTo;

    public OnlineNode(int id, int obj_size) {
        this.id = id;
        setObjSize(obj_size);
        setNonPtr(false);
        representative = NODE_RANK_MIN;
        //owner = new AtomicReference<Iteration>();
        inWorklist = 0;
        copy = new ConcurrentIntSparseBitVector();
        load = new ConcurrentIntSparseBitVector();
        store = new ConcurrentIntSparseBitVector();
        gep = new ConcurrentLongSparseBitVector();
        prevPointsTo = new BddSet();
        pointsTo = new BddSet();
    }

    @Override
    public <T1 extends Node> void ___map(Lambda3Void<Integer, LambdaVoid<T1>, Byte> fn1, LambdaVoid<T1> fn2, byte domain,
                                         byte flags) {
        if (domain == Constraint.COPY) {
            copy.map(fn1, fn2, flags);
            return;
        } else if (domain == Constraint.LOAD) {
            load.map(fn1, fn2, flags);
            return;
        } else if (domain == Constraint.STORE) {
            store.map(fn1, fn2, flags);
            return;
        }
        throw new RuntimeException();
    }

    @Override
    public <T1 extends Node, T2> void ___map(Lambda4Void<Integer, Lambda2Void<T1, T2>, T2, Byte> fn1,
                                             Lambda2Void<T1, T2> fn2, T2 arg2, byte domain, byte flags) {
        if (domain == Constraint.COPY) {
            copy.map(fn1, fn2, arg2, flags);
            return;
        } else if (domain == Constraint.LOAD) {
            load.map(fn1, fn2, arg2, flags);
            return;
        } else if (domain == Constraint.STORE) {
            store.map(fn1, fn2, arg2, flags);
            return;
        }
        throw new RuntimeException();
    }

    @Override
    public <T1 extends Node> void ___map(Lambda3Void<Long, Lambda2Void<Integer, T1>, Byte> fn1,
                                         Lambda2Void<Integer, T1> fn2, byte domain, byte flags) {
        if (domain != Constraint.GEP) {
            throw new RuntimeException();
        }
        gep.map(fn1, fn2, flags);
    }

    @Override
    public boolean ___addNeighbor(int n, byte domain) {
        if (domain == Constraint.COPY) {
            return copy.add(n);
        } else if (domain == Constraint.LOAD) {
            return load.add(n);
        } else if (domain == Constraint.STORE) {
            return store.add(n);
        }
        throw new RuntimeException();
    }

    @Override
    public boolean ___addNeighbor(int n, int edgeData, byte domain) {
        if (domain == Constraint.GEP) {
            return gep.add(((long) n << 32) | edgeData);
        }
        throw new RuntimeException();
    }

    int neighborsSize(byte domain) {
        if (domain == Constraint.COPY) {
            return copy.size();
        } else if (domain == Constraint.LOAD) {
            return load.size();
        } else if (domain == Constraint.STORE) {
            return store.size();
        } else if (domain == Constraint.GEP) {
            return gep.size();
        }
        throw new RuntimeException();
    }

    boolean isNeighborhoodEmpty(byte domain) {
        if (domain == Constraint.COPY) {
            return copy.isEmpty();
        } else if (domain == Constraint.LOAD) {
            return load.isEmpty();
        } else if (domain == Constraint.STORE) {
            return store.isEmpty();
        } else if (domain == Constraint.GEP) {
            return gep.isEmpty();
        }
        throw new RuntimeException();
    }

    // no path compression
    OnlineNode getRep(MultiGraph<OnlineNode> multiGraph) {
        return getRep(multiGraph, MethodFlag.CHECK_CONFLICT);
    }

    OnlineNode getRep(final MultiGraph<OnlineNode> multiGraph, final byte flags) {
        OnlineNode curr = this;
        int currRep = curr.getCurrRepresentative();
        while (currRep < NODE_RANK_MIN) {
            curr = multiGraph.getNode(currRep, flags);
            currRep = curr.getCurrRepresentative();
        }
        return curr;
    }

    public boolean isRep() {
        return getCurrRepresentative() >= NODE_RANK_MIN;
    }

    OnlineNode merge(OnlineNode node2, final boolean copyData, MultiGraph<OnlineNode> multiGraph) {
        return merge(node2, copyData, multiGraph, MethodFlag.CHECK_CONFLICT);
    }

    OnlineNode merge(OnlineNode node2, final boolean copyData, MultiGraph<OnlineNode> multiGraph, byte flags) {
        for (; ; ) {
            OnlineNode node1 = getRep(multiGraph, flags);
            node2 = node2.getRep(multiGraph, flags);
            if (node1 == node2) {
                return node1;
            }
            int rank1 = node1.getCurrRepresentative();
            int rank2 = node2.getCurrRepresentative();
            if (rank1 < NODE_RANK_MIN || rank2 < NODE_RANK_MIN) {
                continue;
            }
            // past this point, we know that both nodes were representatives
            if (rank1 < rank2 || (rank1 == rank2 && node2.id < node1.id)) {
                OnlineNode nodeTmp = node1;
                node1 = node2;
                node2 = nodeTmp;
                int rankTmp = rank2;
                rank2 = rank1;
                rank1 = rankTmp;
            }
            if (!node2.casRepresentative(rank2, node1.id)) {
                continue;
            }
            if (rank1 == rank2) {
                // try to increase rank, if it does not work it is okay
                node1.casRepresentative(rank1, rank1 + 1);
            }
            if (!copyData) {
                // invoked from offline phase
                //System.err.println("    merge " + node1.id  + " <= " +  node2.id);
                return node1;
            } else {
                Statistics.varNodesMergedInOnHcd.incrementAndGet();
            }
            do {
                node1 = node1.getRep(multiGraph, flags);
                boolean constraintsChanged = node1.copy.unionTo(node2.copy);
                constraintsChanged |= node1.load.unionTo(node2.load);
                constraintsChanged |= node1.store.unionTo(node2.store);
                constraintsChanged |= node1.gep.unionTo(node2.gep);
                boolean pointsToChanged = node1.pointsTo.unionTo(node2.pointsTo);
                if (constraintsChanged) {
                    prevPointsToUpdater.set(node1, new BddSet());
                } else {
                    if (!pointsToChanged) {
                        // stop propagating information up to the representative
                        break;
                    }
                }
            } while (!node1.isRep());
            return node1;
        }
    }

    @NotThreadSafe
    OnlineNode serialMerge(OnlineNode node2, final boolean copyData) {
        if (id == node2.id) {
            return this;
        }
        OnlineNode node1 = this;
        assert node1.isRep() && node2.isRep();
        // sequential merge: we can use union-by-rank
        int rank1 = node1.representative;
        int rank2 = node2.representative;
        if (rank1 < rank2) {
            OnlineNode nodeTmp = node1;
            node1 = node2;
            node2 = nodeTmp;
        } else if (rank1 == rank2) {
            node1.representative++;
        }
        node2.representative = node1.id;
        if (node2.isNonPtr()) {
            node1.setNonPtr(true);
        }
        if (!copyData) {
            // invoked from offline phase
            //  System.err.println("    merge " + node1.id  + " <= " +  node2.id);
            return node1;
        }
        node1.pointsTo.serialUnionTo(node2.pointsTo);
        node1.copy.serialUnionTo(node2.copy);
        node1.load.serialUnionTo(node2.load);
        node1.store.serialUnionTo(node2.store);
        node1.gep.serialUnionTo(node2.gep);
        node1.prevPointsTo.clear();
        node2.pointsTo.clear();
        node2.copy.clear();
        node2.load.clear();
        node2.store.clear();
        node2.gep.clear();
        return node1;
    }

    OnlineNode addCopyEdgePropagatePoints2(OnlineNode dst, MultiGraph<OnlineNode> multiGraph) {
        return addCopyEdgePropagatePoints2(dst, multiGraph, MethodFlag.ALL);
    }

    OnlineNode addCopyEdgePropagatePoints2(OnlineNode dst, MultiGraph<OnlineNode> multiGraph, byte flags) {
        if (id == OnlineNode.I2P || dst.id == OnlineNode.I2P || this == dst) {
            return null;
        }
        OnlineNode src = this;
        do {
            src = src.getRep(multiGraph, flags);
            dst = dst.getRep(multiGraph, flags);
            if (src == dst) {
                return null;
            }
            if (multiGraph.addNeighbor(src.id, dst.id, Constraint.COPY, flags)) {
                dst.pointsTo.unionTo(src.pointsTo);
            }
        } while (!src.isRep() || !dst.isRep());
        return dst;
    }

    OnlineNode propagatePointsTo(final IntSet diff, MultiGraph<OnlineNode> multiGraph) {
        return propagatePointsTo(diff, multiGraph, MethodFlag.ALL);
    }

    OnlineNode propagatePointsTo(final IntSet diff, MultiGraph<OnlineNode> multiGraph, byte flags) {
        boolean ret;
        OnlineNode srcRep = this;
        do {
            srcRep = srcRep.getRep(multiGraph, flags);
            ret = srcRep.pointsTo.unionTo(diff);
        } while (!srcRep.isRep());
        return ret ? srcRep : null;
    }

    boolean isNonPtr() {
        return (flagsAndSize & NON_POINTER_MASK) != 0;
    }

    void setNonPtr(boolean nonPtr) {
        if (nonPtr) {
            flagsAndSize |= NON_POINTER_MASK;
        } else {
            flagsAndSize &= (~NON_POINTER_MASK);
        }
    }

    boolean isFunctionNode() {
        return (flagsAndSize & FUNCTION_MASK) != 0;
    }

    void setFunction(boolean function) {
        if (function) {
            flagsAndSize |= FUNCTION_MASK;
        } else {
            flagsAndSize &= (~FUNCTION_MASK);
        }
    }

    int getObjSize() {
        return (int) (flagsAndSize & (~(NON_POINTER_MASK | FUNCTION_MASK)));
    }

    void setObjSize(int size) {
        boolean nonPtr = isNonPtr();
        boolean isFunction = isFunctionNode();
        flagsAndSize = size;
        setNonPtr(nonPtr);
        setFunction(isFunction);
    }

    public static int getLastObjectNode() {
        return lastObjectNode;
    }

    private int getCurrRepresentative() {
        return repUpdater.get(this);
    }

    private boolean casRepresentative(int expected, int next) {
        return repUpdater.compareAndSet(this, expected, next);
    }

    IntSet serialGetPrevPointsTo() {
        return prevPointsTo;
    }

    IntSet getPrevPointsTo() {
        return prevPointsToUpdater.get(this);
    }

    boolean addToWorklist() {
        return inWorklistUpdater.compareAndSet(this, 0, 1);
    }

    void removeFromWorklist() {
        inWorklistUpdater.compareAndSet(this, 1, 0);
    }

    @Override
    public void access(Iteration it, byte flags) {
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(id);
        sb.append(" => ");
        sb.append(pointsTo);
        return sb.toString();
    }

    @Override
    public int hashCode() {
        return id * 31;
    }
}
