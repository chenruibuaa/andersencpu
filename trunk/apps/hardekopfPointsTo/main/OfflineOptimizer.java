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

 File: OfflineOptimizer.java
 */

package hardekopfPointsTo.main;

import galois.objects.IntegerAccumulator;
import galois.objects.IntegerAccumulatorBuilder;
import galois.objects.Mappables;
import galois.objects.MethodFlag;
import galois.runtime.ForeachContext;
import galois.runtime.GaloisRuntime;
import galois.runtime.wl.ChunkedFIFO;
import galois.runtime.wl.Priority;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import util.MutableInteger;
import util.ThreadTimer;
import util.concurrent.BlockingHashSet;
import util.fn.Lambda2Void;
import util.fn.LambdaVoid;
import util.ints.IntSparseBitVector;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import static galois.objects.MethodFlag.NONE;
import static hardekopfPointsTo.main.Constraint.*;
import static hardekopfPointsTo.main.OfflineNode.*;

public final class OfflineOptimizer {

    private static final Logger LOGGER = Logger.getLogger("test.hardekopfPointsTo");
    private static final Logger OPT_LOGGER = Logger.getLogger("test.hardekopfPointsTo.opt");
    private final boolean IS_FINE_LOGGABLE_IN_OPT;

    // time spent in the sequential phases of the algorithm
    long seqTime = 0;

    private final static int FIRST_AFP = 1;

    private final IntAccumulator nextLabel;
    private final TIntIntHashMap hcdTable;
    private int currentDfs;
    private int numRef, firstRef;

    private final int[] main2offline;
    // real number of offline nodes, since the array contains many nulls
    private int currOfflineNodes;

    // real number of constraints, always <= constraints.size()
    private final IntegerAccumulator numConstraints;

    private final MultiGraph<OnlineNode> onlineGraph;
    private MultiGraph<OfflineNode> offlineGraph;

    public OfflineOptimizer(MultiGraph<OnlineNode> onlineGraph, TIntIntHashMap hcdTable) {
        this.onlineGraph = onlineGraph;
        this.hcdTable = hcdTable;
        nextLabel = new IntAccumulator();
        numConstraints = new IntegerAccumulatorBuilder().create(constraints.length);
        main2offline = new int[onlineGraph.size()];
        IS_FINE_LOGGABLE_IN_OPT = OPT_LOGGER.isLoggable(Level.FINE);
    }

    public void constraintOptimization() throws Exception {
        ThreadTimer.Tick start = ThreadTimer.tick();
        offlineOptimization();
        ThreadTimer.Tick end = ThreadTimer.tick();
        long appTime = start.elapsedTime(true, end);
        long totalTime = start.elapsedTime(false, end);
        LOGGER.info("runtime for cons_opt: " + appTime + " ms (including GC: " + totalTime + " ms)");
        // gather statistics before exiting
        Statistics.reducedConstraintCount = getConstraintCount();
        int[] numRep = getNumRep();
        Statistics.repValNodes = numRep[0];
        Statistics.repNodes = numRep[1];
        // nodes merged during HVN = # not-rep nodes that have not been merged during HCD
        Statistics.nodesMergedInHvn = onlineGraph.size() - numRep[1] - Statistics.varNodesMergedInHcd;
        Statistics.addTime(Statistics.Phase.OFFLINE, appTime);
        Statistics.addTime(Statistics.Phase.OFFLINE_SEQ, seqTime);
    }

    private void offlineOptimization() throws Exception {
        clumpAddressTaken();
        if (Configuration.USE_HVN) {
            // do 1 pass of regular HVN, to reduce HU's memory usage.
            hvn(false);
        }
        // TODO : there is a bug? in Hardekopf's implementation. As a result, HRU is merging non ptr-equivalent nodes
        // and the pts-to of a few variables overapproximate the right solution. The verification will fail then if
        // HRU is deactivated, since the correct solution has been computed using HRU in Ben's implementation
        // In other words, we would need a different set of solution files when HRU=off
        if (Configuration.USE_HRU) {
            hr(true, 1000);    // run HRU until it can no longer remove 1000 constraints.
        }
        if (Configuration.USE_HCD) {
            // do HCD after HVN, so that it has fewer nodes to put in the map.
            hcd();
        }
        if (offlineGraph != null) {
            offlineGraph.clear();
        }
    }

    private void clumpAddressTaken() {
        // the constraint + node files were generated after the method was invoked in Ben's code.
    }

    private void hvn(boolean doUnion) throws Exception {
        if (IS_FINE_LOGGABLE_IN_OPT) {
            OPT_LOGGER.fine("***** Starting HVN");
        }
        createOfflineNodes();
        nextLabel.set(onlineGraph.size());
        createOfflineEdges(false);
        ThreadTimer.Tick start = ThreadTimer.tick();
        currentDfs = 1;
        ArrayList<OfflineNode> initialWorklist = new ArrayList<OfflineNode>(currOfflineNodes);
        ArrayList<OfflineNode> dfsStack = new ArrayList<OfflineNode>();
        assert dfsStack.size() == 0 : dfsStack.size();
        for (int i = FIRST_AFP; i < firstRef + numRef; i++) {
            final OfflineNode offlineNode = offlineGraph.getNode(i, NONE);
            if (offlineNode.dfsId == 0) {
                hvnDfs(offlineNode, dfsStack, initialWorklist);
                assert dfsStack.size() == 0 : dfsStack.size();
            }
        }
        ThreadTimer.Tick end = ThreadTimer.tick();
        seqTime += start.elapsedTime(true, end);
        label(initialWorklist, doUnion);
        mergeNodesWithSameLabel();
        mergeConstraints();
    }

    private void createOfflineNodes() throws Exception {
        ThreadTimer.Tick start = ThreadTimer.tick();
        if (IS_FINE_LOGGABLE_IN_OPT) {
            OPT_LOGGER.fine("***** Creating offline graph nodes");
        }
        final int numNodes = onlineGraph.size();
        int numOfflineNodes = 1;
        final int lastObjNode = OnlineNode.getLastObjectNode();
        int i = OnlineNode.FIRST_VAR_NODE;
        while (i <= lastObjNode) {
            OnlineNode node = onlineGraph.getNode(i, NONE);
            int size = node.getObjSize();
            assert size > 0;
            if (node.isFunctionNode()) {
                for (int j = 1; j < size; j++) {
                    int index = i + j;
                    if (onlineGraph.getNode(index, NONE).isRep()) {
                        main2offline[index] = numOfflineNodes;
                        numOfflineNodes++;
                    } else {
                        main2offline[index] = 0;
                    }
                }
            }
            i += size;
        }
        final int firstVal = numOfflineNodes;
        final int numAfp = firstVal - FIRST_AFP;
        main2offline[OnlineNode.P_I2P] = numOfflineNodes;
        numOfflineNodes++;
        for (i = lastObjNode + 1; i < numNodes; i++) {
            OnlineNode node = onlineGraph.getNode(i, NONE);
            if (node.isRep()) {
                main2offline[i] = numOfflineNodes;
                numOfflineNodes++;
            } else {
                main2offline[i] = 0;
            }
        }
        firstRef = numOfflineNodes;
        int numVal = firstRef - firstVal;
        numRef = numVal + numAfp;
        final int totalOfflineNodes = numOfflineNodes + numRef;
        final int to;
        if (offlineGraph == null) {
            OfflineNode[] nodes = new OfflineNode[totalOfflineNodes];
            offlineGraph = new MultiGraph<OfflineNode>(nodes);
            to = totalOfflineNodes;
            OfflineNode.NODE_RANK_MIN = totalOfflineNodes;
        } else {
            to = currOfflineNodes;
        }
        ThreadTimer.Tick end = ThreadTimer.tick();
        seqTime += start.elapsedTime(true, end);
        GaloisRuntime.forall(Mappables.range(0, totalOfflineNodes, Configuration.getChunkSize(totalOfflineNodes)), new LambdaVoid<Integer>() {
            @Override
            public void call(Integer i) {
                if ((i + totalOfflineNodes) < to) {
                    // unused offline nodes are cleared
                    offlineGraph.setNode(i + totalOfflineNodes, null, NONE);
                }
                boolean indirect = i < firstVal || i >= firstRef;
                OfflineNode offlineNode = offlineGraph.getNode(i, NONE);
                if (offlineNode == null) {
                    offlineGraph.setNode(i, new OfflineNode(i, indirect), NONE);
                } else {
                    // reuse existing offline nodes => avoids excessive object creation
                    offlineNode.reset(i, indirect);
                }
            }
        });
        currOfflineNodes = totalOfflineNodes;
        if (IS_FINE_LOGGABLE_IN_OPT) {
            OPT_LOGGER.fine("  " + FIRST_AFP + " firstAFP, " + firstVal + " firstVAL, " + firstRef + " firstREF");
            OPT_LOGGER.fine("  " + numAfp + " AFP, " + numVal + " VAL, " + numRef + " REF");
        }
    }

    private void createOfflineEdges(final boolean hcd) throws Exception {
        if (IS_FINE_LOGGABLE_IN_OPT) {
            OPT_LOGGER.fine("***** Adding offline constraint edges");
        }
        int initialGep = Statistics.initialConstraintCount[Constraint.GEP].get();
        final ConcurrentHashMap<Long, Integer> gep2label = new ConcurrentHashMap<Long, Integer>(initialGep / 4, Configuration.CAPACITY, 64);
        int numConstraints = constraints.length;
        final boolean storeOutgoingEdges = (Configuration.USE_HCD | Configuration.USE_HRU);
        GaloisRuntime.forall(Mappables.range(0, numConstraints, Configuration.getChunkSize(numConstraints)), new LambdaVoid<Integer>() {
            @Override
            public void call(Integer i) {
                Constraint constraint = constraints[i];
                if (constraint == null) {
                    return;
                }
                int dest = constraint.dst;
                int src = constraint.src;
                // src and dest are representatives because we previously updated the constraints
                assert onlineGraph.getNode(src, NONE).isRep() && onlineGraph.getNode(dest, NONE).isRep();
                int offlineDest = main2offline[dest];
                if (offlineDest == 0) {
                    assert onlineGraph.getNode(dest, NONE).getObjSize() > 0;
                    return;
                }
                int offlineSrc = main2offline[src];
                assert (offlineSrc > 0 || onlineGraph.getNode(src, NONE).getObjSize() > 0);
                int refOfflineSrc = ref(offlineSrc);
                int refOfflineDest = ref(offlineDest);
                if (constraint.type == ADDR_OF) {
                    if (!hcd) {
                        offlineGraph.getNode(offlineDest, NONE).label.concurrentAdd(src);
                        if (offlineSrc > 0) {
                            // D = &S => S -> *D
                            offlineGraph.addNeighbor(refOfflineDest, offlineSrc, IMPLICIT_IN_EDGES, NONE);
                        }
                    }
                } else if (constraint.type == COPY) {
                    //D = S  => edge S -> D => *S -> *D.
                    if (offlineSrc > 0) {
                        if (storeOutgoingEdges) {
                            offlineGraph.addNeighbor(offlineSrc, offlineDest, OUT_EDGES, NONE);
                        }
                        offlineGraph.addNeighbor(offlineDest, offlineSrc, IN_EDGES, NONE);
                        if (!hcd) {
                            offlineGraph.addNeighbor(refOfflineDest, refOfflineSrc, IMPLICIT_IN_EDGES, NONE);
                        }
                    } else {
                        if (!hcd) {
                            offlineGraph.getNode(offlineDest, NONE).setIndirect(true);
                        }
                    }
                } else if (constraint.type == LOAD) {
                    if (constraint.offset > 0) {
                        //D = *S + k => D indirect
                        if (!hcd) {
                            offlineGraph.getNode(offlineDest, NONE).setIndirect(true);
                        }
                    } else {
                        //D = *S => edge *S -> *D
                        assert offlineSrc > 0;
                        if (storeOutgoingEdges)
                            offlineGraph.addNeighbor(refOfflineSrc, offlineDest, OUT_EDGES, NONE);
                        offlineGraph.addNeighbor(offlineDest, refOfflineSrc, IN_EDGES, NONE);
                    }
                } else if (constraint.type == STORE) {
                    //*D + k = *S is ignored
                    if (constraint.offset == 0) {
                        //*D = S => edge S -> *D
                        assert offlineSrc > 0;
                        if (storeOutgoingEdges)
                            offlineGraph.addNeighbor(offlineSrc, refOfflineDest, OUT_EDGES, NONE);
                        offlineGraph.addNeighbor(refOfflineDest, offlineSrc, IN_EDGES, NONE);
                    }
                } else if (constraint.type == GEP) {
                    //D = gep S k
                    if (!hcd) {
                        Long key = ((long) src << 32) | constraint.offset;
                        int currLabel = nextLabel.add(1, NONE);
                        Integer prev = gep2label.putIfAbsent(key, currLabel);
                        currLabel = prev == null ? currLabel : prev;
                        offlineGraph.getNode(offlineDest, NONE).label.concurrentAdd(currLabel);
                    }
                } else {
                    throw new RuntimeException();
                }
            }
        });
    }

    private void hvnDfs(final OfflineNode offlineNode, final ArrayList<OfflineNode> dfsStack, final ArrayList<OfflineNode> initialWorklist) {
        assert !offlineNode.isSccRoot() && offlineNode.isRep();
        int ourDfs = currentDfs;
        currentDfs++;
        offlineNode.dfsId = ourDfs;
        LambdaVoid<OfflineNode> fn = new LambdaVoid<OfflineNode>() {
            @Override
            public void call(OfflineNode d) {
                // no self-edges
                assert d != offlineNode;
                assert offlineNode.isRep();
                OfflineNode dst = d.getRep(offlineGraph, NONE);
                if (offlineNode == dst || dst.isSccRoot()) {
                    return;
                }
                if (dst.dfsId == 0) {
                    hvnDfs(dst, dfsStack, initialWorklist);
                }
                offlineNode.dfsId = Math.min(offlineNode.dfsId, dst.dfsId);
                assert offlineNode.isRep();
            }
        };
        offlineGraph.map(offlineNode, fn, IN_EDGES, NONE);
        offlineGraph.map(offlineNode, fn, IMPLICIT_IN_EDGES, NONE);
        assert offlineNode.isRep();
        if (offlineNode.dfsId == ourDfs) {
            OfflineNode sccRoot = offlineNode;
            while (!dfsStack.isEmpty()) {
                int last = dfsStack.size() - 1;
                final OfflineNode offlineNodeTop = dfsStack.get(last);
                if (offlineNodeTop.dfsId < ourDfs) {
                    break;
                }
                dfsStack.remove(last);
                sccRoot = sccRoot.merge(offlineNodeTop);
            }
            sccRoot.setSccRoot(true);
            if (sccRoot.computeIncomingDegree(offlineGraph, NONE) == 0) {
                initialWorklist.add(sccRoot);
            }
        } else {
            dfsStack.add(offlineNode);
        }
    }

    private void label(final ArrayList<OfflineNode> initialWorklist, final boolean doUnion) throws Exception {
        final ConcurrentHashMap<IntSparseBitVector, Integer> hvnTable = doUnion ? null : new ConcurrentHashMap<IntSparseBitVector, Integer>(1024, Configuration.CAPACITY, Configuration.CONCURRENCY_LEVEL);
        GaloisRuntime.foreach(initialWorklist, new Lambda2Void<OfflineNode, ForeachContext<OfflineNode>>() {
            @Override
            public void call(final OfflineNode src, final ForeachContext<OfflineNode> ctx) {
                assert src.isRep();
                // label this node
                if (doUnion) {
                    huLabel(src);
                } else {
                    hvnLabel(src, hvnTable);
                }
                final IntSparseBitVector seen = new IntSparseBitVector();
                offlineGraph.map(src, new LambdaVoid<OfflineNode>() {
                    @Override
                    public void call(OfflineNode d) {
                        OfflineNode dst = d.getRep(offlineGraph, NONE);
                        if (src != dst && seen.add(dst.dfsId) && dst.decrementIncomingInDegree() == 0) {
                            ctx.add(dst, MethodFlag.NONE);
                        }
                    }
                }, OUT_EDGES, NONE);
            }

            private void hvnLabel(OfflineNode offlineNode, final ConcurrentHashMap<IntSparseBitVector, Integer> hvnTable) {
                IntSparseBitVector label = offlineNode.label;
                assert offlineNode.isRep() && offlineNode.isSccRoot();
                if (offlineNode.isIndirect()) {
                    label.clear();
                    label.add(nextLabel.add(1, NONE));
                    return;
                }
                addIncomingLabels(offlineNode);
                if (!label.isSingleton()) {
                    int num = nextLabel.add(1, NONE);
                    Integer prevLabel = hvnTable.putIfAbsent(label, num);
                    if (prevLabel != null) {
                        num = prevLabel;
                    }
                    offlineNode.label = new IntSparseBitVector();
                    offlineNode.label.add(num);
                }
                assert offlineNode.label.isSingleton();
            }

            private void huLabel(OfflineNode offlineNode) {
                assert offlineNode.isRep() && offlineNode.isSccRoot();
                IntSparseBitVector label = offlineNode.label;
                if (offlineNode.isIndirect()) {
                    label.add(nextLabel.add(1, NONE));
                }
                addIncomingLabels(offlineNode);
            }

            private void addIncomingLabels(final OfflineNode offlineNode1) {
                final IntSparseBitVector seen = new IntSparseBitVector();
                final IntSparseBitVector label1 = offlineNode1.label;
                offlineGraph.map(offlineNode1, new LambdaVoid<OfflineNode>() {
                    @Override
                    public void call(OfflineNode n) {
                        OfflineNode offlineNode2 = n.getRep(offlineGraph, NONE);
                        assert offlineNode2.isRep();
                        if (offlineNode2 == offlineNode1 || !seen.add(offlineNode2.id)) {
                            return;
                        }
                        assert offlineNode2.dfsId > 0;
                        IntSparseBitVector label2 = offlineNode2.label;
                        assert !label2.isEmpty();
                        if (!label2.contains(0)) {
                            label1.unionTo(label2);
                        }
                    }
                }, IN_EDGES, NONE);
                if (label1.isEmpty()) {
                    label1.add(0);
                }
            }
        }, Priority.first(ChunkedFIFO.class, Configuration.getChunk()));
    }

    private void mergeNodesWithSameLabel() throws Exception {
        final int numNodes = onlineGraph.size();
        final ConcurrentHashMap<IntSparseBitVector, OnlineNode> label2node = new ConcurrentHashMap<IntSparseBitVector, OnlineNode>(numNodes / 4, Configuration.CAPACITY, Configuration.CONCURRENCY_LEVEL);
        GaloisRuntime.forall(Mappables.range(0, numNodes, Configuration.getChunkSize(numNodes)), new LambdaVoid<Integer>() {
            @Override
            public void call(Integer i) {
                OnlineNode node = onlineGraph.getNode(i, NONE);
                if (main2offline[i] == 0) {
                    return;
                }
                OfflineNode offlineNode = offlineGraph.getNode(main2offline[i], NONE).getRep(offlineGraph, NONE);
                IntSparseBitVector label = offlineNode.label;
                assert !label.isEmpty();
                boolean nonPtr = label.contains(0);
                assert (nonPtr && label.isSingleton()) || !nonPtr;
                if (nonPtr) {
                    node.setNonPtr(true);
                    return;
                }
                OnlineNode nodeWithSameLabel = label2node.putIfAbsent(label, node);
                if (nodeWithSameLabel != null) {
                    assert nodeWithSameLabel != node;
                    nodeWithSameLabel.merge(node, false, onlineGraph, NONE);
                }
            }
        });
    }

    private void mergeConstraints() throws Exception {
        final int constraintSize = Constraint.constraints.length;
        final BlockingHashSet<Constraint> constraintsSeen = new BlockingHashSet<Constraint>(constraintSize / 2, Configuration.CAPACITY, Configuration.CONCURRENCY_LEVEL);
        GaloisRuntime.forall(Mappables.range(0, constraintSize, Configuration.getChunkSize(constraintSize)), new LambdaVoid<Integer>() {
            @Override
            public void call(Integer i) {
                Constraint constraint = constraints[i];
                if (constraint == null) {
                    return;
                }
                OnlineNode src = onlineGraph.getNode(constraint.src, NONE).getRep(onlineGraph, NONE);
                OnlineNode dst = onlineGraph.getNode(constraint.dst, NONE).getRep(onlineGraph, NONE);
                if ((constraint.type == COPY && src == dst) || src.isNonPtr() || dst.isNonPtr()) {
                    constraints[i] = null;
                    numConstraints.add(-1, NONE);
                    return;
                }
                constraint.dst = dst.id;
                if (constraint.type != ADDR_OF) {
                    constraint.src = src.id;
                }
                if (!constraintsSeen.add(constraint)) {
                    constraints[i] = null;
                    numConstraints.add(-1, NONE);
                }
                //TODO: handle indirect calls...
            }
        });
    }

    private void hr(boolean doUnion, int target) throws Exception {
        int prevConstraints;
        if (IS_FINE_LOGGABLE_IN_OPT) {
            OPT_LOGGER.info("  running HR" + (doUnion ? "U" : "") + ", constraint count:  " + numConstraints.get());
        }
        do {
            prevConstraints = numConstraints.get();
            hvn(true);
            if (IS_FINE_LOGGABLE_IN_OPT) {
                OPT_LOGGER.info("  constraints reduced to " + numConstraints.get());
            }
        } while (prevConstraints - numConstraints.get() >= target);
    }

    private void hcd() throws Exception {
        if (IS_FINE_LOGGABLE_IN_OPT) {
            OPT_LOGGER.fine("");
            OPT_LOGGER.fine("***** Starting HCD");
        }
        createOfflineNodes();
        createOfflineEdges(true);
        ThreadTimer.Tick start = ThreadTimer.tick();
        // not worth parallelizing
        for (int i = 0; i < main2offline.length; i++) {
            int n = main2offline[i];
            if (n > 0) {
                offlineGraph.getNode(n, NONE).setMainNode(i);
                offlineGraph.getNode(ref(n), NONE).setMainNode(i);
            }
        }
        ArrayList<OfflineNode> dfsStack = new ArrayList<OfflineNode>();
        currentDfs = 1;
        for (int i = FIRST_AFP; i < firstRef + numRef; i++) {
            if (offlineGraph.getNode(i, NONE).dfsId == 0) {
                hcdDfs(i, dfsStack);
            }
        }
        ThreadTimer.Tick end = ThreadTimer.tick();
        seqTime += start.elapsedTime(true, end);
        assert dfsStack.size() == 0;
        mergeConstraints();
        Statistics.hcdSize = hcdTable.size();
    }

    private void hcdDfs(int n, final ArrayList<OfflineNode> dfsStack) {
        assert n > 0;
        final OfflineNode offlineNode = offlineGraph.getNode(n, NONE);
        assert !offlineNode.isSccRoot() && offlineNode.isRep();
        int ourDfs = currentDfs;
        currentDfs++;
        offlineNode.dfsId = ourDfs;
        offlineGraph.map(offlineNode, new LambdaVoid<OfflineNode>() {
            @Override
            public void call(OfflineNode offlineNode2) {
                assert offlineNode2.isRep();
                if (offlineNode2.isSccRoot()) {
                    return;
                }
                if (offlineNode2.dfsId == 0) {
                    hcdDfs(offlineNode2.id, dfsStack);
                }
                offlineNode.dfsId = Math.min(offlineNode.dfsId, offlineNode2.dfsId);
            }
        }, IN_EDGES, NONE);
        assert offlineNode.isRep();
        if (offlineNode.dfsId != ourDfs) {
            dfsStack.add(offlineNode);
            return;
        }
        //Record all nodes in our SCC (the root is not on the stack).
        TIntArrayList scc = new TIntArrayList();
        scc.add(n);
        TIntArrayList var = new TIntArrayList();
        if (n < firstRef) {
            var.add(n);
        }
        while (!dfsStack.isEmpty()) {
            int last = dfsStack.size() - 1;
            OfflineNode topNode = dfsStack.get(last);
            int n2 = topNode.id;
            assert n2 != n;
            if (topNode.dfsId < ourDfs) {
                break;
            }
            dfsStack.remove(last);
            scc.add(n2);
            if (n2 < firstRef) {
                var.add(n2);
            }
        }
        if (scc.size() == 1) {
            offlineNode.setSccRoot(true);
            return;
        }
        assert var.size() > 0;
        int varRep = offlineGraph.getNode(var.getQuick(0), NONE).getMainNode();
        int varSize = var.size();
        for (int i = 1; i < varSize; i++) {
            final int mainNode = offlineGraph.getNode(var.getQuick(i), NONE).getMainNode();
            onlineGraph.getNode(varRep, NONE).serialMerge(onlineGraph.getNode(mainNode, NONE), false);
            OnlineNode newRep = findFurtherCycles(onlineGraph.getNode(varRep, NONE), onlineGraph.getNode(mainNode, NONE));
            varRep = newRep.id;
            Statistics.varNodesMergedInHcd++;
        }
        int sccSize = scc.size();
        for (int i = 0; i < sccSize; i++) {
            int sccN = scc.getQuick(i);
            assert sccN > 0;
            OfflineNode thisEyes = offlineGraph.getNode(sccN, NONE);
            thisEyes.setSccRoot(true);
            if (sccN >= firstRef) {
                hcdTable.put(thisEyes.getMainNode(), varRep);
            }
        }
    }

    private OnlineNode findFurtherCycles(final OnlineNode node1, final OnlineNode node2) {
        int n1 = node1.id;
        int n2 = node2.id;
        int hv2 = hcdTable.get(n2);
        if (hv2 != 0) {
            int hv1 = hcdTable.get(n1);
            if (hv1 == 0) {
                // *n1 is the same as *n2 (because n1/n2 have to be pointer-equivalent
                // to get merged), so *n1 would be in a cycle with HV2.
                hcdTable.put(n1, hv2);
            } else {
                // If we had offline cycles (*n1, rep1) and (*n2, rep2), and we know that *n1 and *n2
                // are the same, it means rep1 and rep2 will be in the same SCC.
                OnlineNode rep1 = onlineGraph.getNode(hv1, NONE).getRep(onlineGraph, NONE);
                OnlineNode rep2 = onlineGraph.getNode(hv2, NONE).getRep(onlineGraph, NONE);
                if (rep1 != rep2) {
                    rep1.serialMerge(rep2, false);
                    Statistics.varNodesMergedInHcd++;
                }
            }
            hcdTable.remove(n2);
        }
        return onlineGraph.getNode(n1, NONE).getRep(onlineGraph, NONE);
    }

    private int ref(final int num) {
        return num - FIRST_AFP + firstRef;
    }

    private long[] getConstraintCount() {
        long[] freq = new long[6];
        for (Constraint constraint : constraints) {
            if (constraint != null) {
                freq[constraint.type]++;
                freq[5]++;
            }
        }
        return freq;
    }

    private int[] getNumRep() {
        final int lastObjNode = OnlineNode.getLastObjectNode();
        final MutableInteger total = new MutableInteger(0);
        final MutableInteger obj = new MutableInteger(0);
        onlineGraph.map(new LambdaVoid<OnlineNode>() {
            @Override
            public void call(OnlineNode node) {
                if (node.isRep()) {
                    total.incrementAndGet();
                    if (node.id > lastObjNode) {
                        obj.incrementAndGet();
                    }
                }
            }
        });
        return new int[]{obj.get(), total.get()};
    }

    // the interface @link{galois.objects.Accumulator} does not return the value
    // of the accumulator on calls to add.
    private static class IntAccumulator extends AtomicInteger {

        IntAccumulator() {
            this(0);
        }

        IntAccumulator(int x) {
            super(x);
        }

        public int add(int x) {
            return add(x, MethodFlag.ALL);
        }

        public int add(int x, byte flags) {
            return super.addAndGet(x);
        }
    }
}

