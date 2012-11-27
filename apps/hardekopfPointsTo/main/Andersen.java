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

 File: Andersen.java
 */


package hardekopfPointsTo.main;

import galois.objects.Mappables;
import galois.objects.MethodFlag;
import galois.runtime.AbstractForeachContext;
import galois.runtime.ForeachContext;
import galois.runtime.GaloisRuntime;
import galois.runtime.wl.ChunkedFIFO;
import galois.runtime.wl.FIFO;
import galois.runtime.wl.Priority;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import util.MutableBoolean;
import util.MutableInteger;
import util.MutableReference;
import util.ThreadTimer;
import util.concurrent.ConcurrentGrowingList;
import util.fn.Lambda2Void;
import util.fn.Lambda3Void;
import util.fn.Lambda4Void;
import util.fn.LambdaVoid;
import util.ints.IntSet;
import util.ints.IntSparseBitVector;
import util.ints.LongSparseBitVector;
import util.ints.bdd.*;

import java.util.*;
import java.util.logging.Logger;

import static galois.objects.MethodFlag.NONE;

final class Andersen implements Lambda2Void<OnlineNode, ForeachContext<OnlineNode>> {
    public static final Logger LOGGER = Logger.getLogger("test.hardekopfPointsTo");
    private static final byte FLAG = MethodFlag.NONE;

    private static final int SCC_ROOT_MASK = 1 << 31;
    private int currentDfs;
    private int repNodes;

    private final MultiGraph<OnlineNode> onlineGraph;
    private BddNode[] geps;
    private BddPair gep2pts;
    private final TIntIntHashMap hcdTable;

    public Andersen(MultiGraph<OnlineNode> onlineGraph, TIntIntHashMap hcdTable) throws Exception {
        this.onlineGraph = onlineGraph;
        this.hcdTable = hcdTable;
    }

    public long pts_init() {
        ThreadTimer.Tick start = ThreadTimer.tick();
        int numNodes = onlineGraph.size();
        // based on a regression on the number of remaining vars & constraints, figure out the right initial
        // size for the BDD (expressed as its logarithm base 2).
        long initNodeTableSize = Math.round(Math.log(-188670.1876f + 15.84011909f * (float) Statistics.repValNodes + 27.22086645f * (float) Statistics.reducedConstraintCount[5]) / Math.log(2.0));
        initNodeTableSize = Math.max(initNodeTableSize, 8);
        // increase the size conservatively, in case we use a high number of threads
        // TODO: take into account the number of threads in the regression model
        initNodeTableSize += 1;
        //System.err.println("Log of the initial node table size: " + initNodeTableSize);
        // the # of entries for the iterator cache is a function of the initial node table
        BddDomain.setup(1 << initNodeTableSize, Configuration.CONCURRENCY_LEVEL, 1 << (initNodeTableSize / 2));
        if (Configuration.USE_BDD_ADDER) {
            BddDomain.extDomain(new long[]{numNodes, numNodes});
        } else {
            BddDomain.extDomain(new long[]{numNodes});
        }
        BddDomain ptsDomain = BddDomain.getDomain(0);
        BddSet.setDomain(ptsDomain);
        if (Configuration.USE_BDD_ADDER) {
            BddDomain gepDomain = BddDomain.getDomain(1);
            gep2pts = new BddPair();
            gep2pts.set(gepDomain, ptsDomain);
            TreeSet<Integer> validOffsets = new TreeSet<Integer>(new Comparator<Integer>() {
                // store offsets in reverse order
                @Override
                public int compare(Integer int1, Integer int2) {
                    return int2 - int1;
                }
            });
            for (Constraint constraint : Constraint.constraints) {
                if (constraint != null && constraint.type == Constraint.GEP) {
                    validOffsets.add(constraint.offset);
                }
            }
            validOffsets.remove(0);
            ArrayList<BddSet> offNodes = new ArrayList<BddSet>();
            int maxSize = createOffsetNodes(validOffsets, offNodes);
            geps = createAdder(validOffsets, offNodes, maxSize);
        }
        ThreadTimer.Tick end = ThreadTimer.tick();
        long appTime = start.elapsedTime(true, end);
        long totalTime = start.elapsedTime(false, end);
        Statistics.addTime(Statistics.Phase.ONLINE_SEQ, appTime);
        LOGGER.fine("runtime for pts_init: " + appTime + " ms (including GC: " + totalTime + " ms)");
        return appTime;
    }

    private int createOffsetNodes(final TreeSet<Integer> validOffsets, final ArrayList<BddSet> offNodes) {
        final MutableInteger maxOffset = new MutableInteger();
        offNodes.add(null);
        onlineGraph.map(new LambdaVoid<OnlineNode>() {
            @Override
            public void call(OnlineNode node) {
                int size = node.getObjSize();
                if (size < 2) {
                    return;
                }
                int offset = validOffsets.ceiling(size - 1);
                if (offset > 0) {
                    if (offset > maxOffset.get()) {
                        for (int j = maxOffset.get(); j < offset; j++) {
                            offNodes.add(new BddSet());
                        }
                        maxOffset.set(offset);
                    }
                    BddSet bddSet = offNodes.get(offset);
                    bddSet.serialAdd(node.id);
                }
            }
        });
        return maxOffset.incrementAndGet();
    }

    private BddNode[] createAdder(final TreeSet<Integer> validOffsets, final ArrayList<BddSet> offNodes, int maxSize) {
        BddNode[] result = new BddNode[maxSize];
        BddDomain ptsDomain = BddDomain.getDomain(0);
        BddDomain gepDomain = BddDomain.getDomain(1);
        BitVector vpts = BitVector.varfdd(ptsDomain);
        int numBitsVpts = vpts.bitNum();
        BitVector vgep = BitVector.varfdd(gepDomain);
        BddNode om = Bdd.ZERO;
        for (int offset : validOffsets) {
            BddSet nodesWithThisSize = offNodes.get(offset);
            om = Bdd.or(om, nodesWithThisSize.getRoot());
            BitVector add = BitVector.add(vpts, BitVector.con(numBitsVpts, offset));
            BddNode f = BitVector.equ(vgep, add);
            result[offset] = Bdd.and(f, om);
        }
        return result;
    }

    int getAvgTimeAt(byte phase) {
        return Statistics.ruleTimes[phase].get() / (GaloisRuntime.getRuntime().getMaxThreads());
    }

    void analyze() throws Exception {
        ThreadTimer.Tick start = ThreadTimer.tick();
        repNodes = Statistics.repNodes;
        pts_init();
        List<OnlineNode> initialWorklist = loadContraints();
        if (Configuration.BEN_ANALYSIS) {
            hardekopfAnalysis(initialWorklist);
        } else {
            waveAnalysis(initialWorklist);
        }
        ThreadTimer.Tick end = ThreadTimer.tick();
        long appTime = start.elapsedTime(true, end);
        long totalTime = start.elapsedTime(false, end);
        LOGGER.info("runtime for solve: " + appTime + " ms (including GC: " + totalTime + " ms)");
        LOGGER.info("    create: " + Statistics.ruleTimes[Statistics.CREATE_GRAPH].get() + " ms");
        LOGGER.info("    update: " + getAvgTimeAt(Statistics.UPDATE_DIFF_PTS) + " ms");
        LOGGER.info("       hcd: " + getAvgTimeAt(Statistics.HCD) + " ms");
        LOGGER.info("      copy: " + getAvgTimeAt(Constraint.COPY) + " ms");
        LOGGER.info("      load: " + getAvgTimeAt(Constraint.LOAD) + " ms");
        LOGGER.info("     store: " + getAvgTimeAt(Constraint.STORE) + " ms");
        LOGGER.info("       gep: " + getAvgTimeAt(Constraint.GEP) + " ms");
        // compute statistics
        Statistics.addTime(Statistics.Phase.ONLINE, appTime);
    }

    private List<OnlineNode> loadContraints() throws Exception {
        long startTime = milliTime();
        final ConcurrentGrowingList<OnlineNode> initialWorklist = new ConcurrentGrowingList<OnlineNode>(repNodes);
        final Constraint[] constraints = Constraint.constraints;
        int numConstraints = constraints.length;
        GaloisRuntime.forall(Mappables.range(0, numConstraints, Configuration.getChunkSize(numConstraints)), new LambdaVoid<Integer>() {
            @Override
            public void call(Integer index) {
                int i = index;
                Constraint constraint = constraints[i];
                if (constraint == null) {
                    return;
                }
                assert onlineGraph.getNode(constraint.src, NONE).isRep();
                assert onlineGraph.getNode(constraint.dst, NONE).isRep();
                OnlineNode node = null;
                if (constraint.type == Constraint.ADDR_OF) {
                    // DEST = &SRC
                    node = onlineGraph.getNode(constraint.dst, NONE);
                    node.pointsTo.add(constraint.src);
                    if (node.addToWorklist()) {
                        initialWorklist.add(node);
                    }
                } else if (constraint.type == Constraint.COPY) {
                    // DEST = SRC
                    node = onlineGraph.getNode(constraint.src, NONE);
                    onlineGraph.addNeighbor(constraint.src, constraint.dst, Constraint.COPY, NONE);
                } else if (constraint.type == Constraint.LOAD) {
                    // DEST = *SRC
                    if (constraint.offset > 0) {
                        // TODO: loads and stores with offset
                        return;
                    }
                    node = onlineGraph.getNode(constraint.src, NONE);
                    onlineGraph.addNeighbor(constraint.src, constraint.dst, Constraint.LOAD, NONE);
                } else if (constraint.type == Constraint.STORE) {
                    // *DEST = SRC
                    if (constraint.offset > 0) {
                        // TODO: loads and stores with offset
                        return;
                    }
                    node = onlineGraph.getNode(constraint.dst, NONE);
                    onlineGraph.addNeighbor(constraint.dst, constraint.src, Constraint.STORE, NONE);
                } else if (constraint.type == Constraint.GEP) {
                    node = onlineGraph.getNode(constraint.src, NONE);
                    onlineGraph.addNeighbor(constraint.src, constraint.dst, constraint.offset, Constraint.GEP, NONE);
                } else {
                    throw new RuntimeException();
                }
                constraints[i] = null;
            }
        });
        long endTime = milliTime();
        Statistics.ruleTimes[Statistics.CREATE_GRAPH].add((int) (endTime - startTime), MethodFlag.NONE);
        return initialWorklist;
    }

    private void hardekopfAnalysis(List<OnlineNode> initialWorklist) throws Exception {
        Priority.Rule priority = Priority.first(ChunkedFIFO.class, Configuration.getChunk()).then(FIFO.class);
        GaloisRuntime.foreach(initialWorklist, this, priority);
    }

    private void waveAnalysis(List<OnlineNode> changedNodes) throws Exception {
        while (!changedNodes.isEmpty()) {
            Iterable<OnlineNode> sccRoots = waveCollapseScc(changedNodes);
            List<OnlineNode> changedSccRoots = wavePropagate(sccRoots);
            changedNodes = waveSolve(changedSccRoots);
        }
    }

    private Iterable<OnlineNode> waveCollapseScc(final List<OnlineNode> changedNodes) {
        ThreadTimer.Tick start = ThreadTimer.tick();
        currentDfs = 1;
        final int size = changedNodes.size();
        final ArrayDeque<OnlineNode> initialWorklist = new ArrayDeque<OnlineNode>(size);
        final ArrayList<OnlineNode> dfsStack = new ArrayList<OnlineNode>();
        final int[] flagsAndDfsId = new int[onlineGraph.size()];
        for (int i = 0; i < size; i++) {
            OnlineNode node = changedNodes.get(i);
            if (node.isRep() && getDfsId(node, flagsAndDfsId) == 0) {
                dfsRec(node, dfsStack, flagsAndDfsId, initialWorklist);
                assert dfsStack.size() == 0 : dfsStack.size();
            }
        }
        ThreadTimer.Tick end = ThreadTimer.tick();
        long appTime = start.elapsedTime(true, end);
        Statistics.accTime(Statistics.Phase.ONLINE_SEQ, appTime);
        return initialWorklist;
    }

    private void dfsRec(final OnlineNode node, final ArrayList<OnlineNode> dfsStack, final int[] flagsAndDfsId, final ArrayDeque<OnlineNode> initialWorklist) {
        assert node.isRep();
        int ourDfs = currentDfs++;
        setDfsId(node, ourDfs, flagsAndDfsId);
        onlineGraph.map(node, new LambdaVoid<OnlineNode>() {
            @Override
            public void call(final OnlineNode d) {
                OnlineNode dst = d.getRep(onlineGraph, NONE);
                if (node == dst || isSccRoot(dst, flagsAndDfsId)) {
                    return;
                }
                if (getDfsId(dst, flagsAndDfsId) == 0) {
                    dfsRec(dst, dfsStack, flagsAndDfsId, initialWorklist);
                }
                final int nodeDfsId = getDfsId(node, flagsAndDfsId);
                final int dstDfsId = getDfsId(dst, flagsAndDfsId);
                if (dstDfsId < nodeDfsId) {
                    setDfsId(node, dstDfsId, flagsAndDfsId);
                }
            }
        }, Constraint.COPY, NONE);
        assert node.isRep();
        if (getDfsId(node, flagsAndDfsId) == ourDfs) {
            OnlineNode sccRoot = node;
            while (!dfsStack.isEmpty()) {
                int last = dfsStack.size() - 1;
                final OnlineNode nodeOnTop = dfsStack.get(last);
                if (getDfsId(nodeOnTop, flagsAndDfsId) < ourDfs) {
                    break;
                }
                dfsStack.remove(last);
                repNodes--;
                sccRoot = sccRoot.serialMerge(nodeOnTop, true);
            }
            setSccRoot(sccRoot, true, flagsAndDfsId);
            sccRoot.removeFromWorklist();
            if (!sccRoot.serialGetPrevPointsTo().equals(sccRoot.pointsTo)) {
                initialWorklist.addFirst(sccRoot);
            }
        } else {
            dfsStack.add(node);
        }
    }

    private static boolean isSccRoot(OnlineNode node, int[] dfsFlagsAndDfsId) {
        return (dfsFlagsAndDfsId[node.id] & SCC_ROOT_MASK) != 0;
    }

    private static void setSccRoot(OnlineNode node, boolean sccRoot, int[] dfsFlagsAndDfsId) {
        int id = node.id;
        if (sccRoot) {
            dfsFlagsAndDfsId[id] |= SCC_ROOT_MASK;
        } else {
            dfsFlagsAndDfsId[id] &= (~SCC_ROOT_MASK);
        }
    }

    private static int getDfsId(OnlineNode node, int[] dfsFlagsAndDfsId) {
        return dfsFlagsAndDfsId[node.id] & (~SCC_ROOT_MASK);
    }

    private static void setDfsId(OnlineNode node, int dfsId, int[] dfsFlagsAndDfsId) {
        int mask = dfsFlagsAndDfsId[node.id] & SCC_ROOT_MASK;
        dfsFlagsAndDfsId[node.id] = dfsId | mask;
    }

    private List<OnlineNode> wavePropagate(final Iterable<OnlineNode> nodesWhosePointsToChanged) throws Exception {
        final ConcurrentGrowingList<OnlineNode> ret = new ConcurrentGrowingList<OnlineNode>(repNodes);
        GaloisRuntime.foreach(nodesWhosePointsToChanged, new Lambda2Void<OnlineNode, ForeachContext<OnlineNode>>() {
            @Override
            public void call(final OnlineNode node, final ForeachContext<OnlineNode> wl) {
                assert node.isRep();
                final BddSet diffPointsTo = node.pointsTo.clone();
                diffPointsTo.serialDiffTo((BddSet) node.getPrevPointsTo());
                if (diffPointsTo.isEmpty()) {
                    return;
                }
                if (node.addToWorklist()) {
                    ret.add(node);
                }
                final IntSparseBitVector seen = new IntSparseBitVector();
                onlineGraph.map(node, new LambdaVoid<OnlineNode>() {
                    @Override
                    public void call(final OnlineNode d) {
                        OnlineNode dst = d.getRep(onlineGraph, NONE);
                        if (node != dst && seen.add(dst.id) && dst.pointsTo.unionTo(diffPointsTo)) {
                            wl.add(dst, NONE);
                        }
                    }
                }, Constraint.COPY, NONE);
            }
        }, Priority.first(FIFO.class));
        return ret;
    }

    private List<OnlineNode> waveSolve(List<OnlineNode> nodesWhosePointsToChanged) throws Exception {
        final int size = nodesWhosePointsToChanged.size();
        final Collection2ForEachContextAdapter<OnlineNode> adapter = new Collection2ForEachContextAdapter<OnlineNode>(repNodes);
        GaloisRuntime.forall(Mappables.fromList(nodesWhosePointsToChanged, Configuration.getChunkSize(size)), new LambdaVoid<OnlineNode>() {
            @Override
            public void call(OnlineNode node) {
                Andersen.this.call(node, adapter);
            }
        });
        return adapter.get();
    }

    private long milliTime() {
        return System.nanoTime() / 1000000;
    }

    @Override
    public void call(final OnlineNode node, final ForeachContext<OnlineNode> worklist) {
        Statistics.nodeRuns.add(1, MethodFlag.NONE);
        node.removeFromWorklist();
        // UPDATE DIFF PTS RULE
        long startTime = milliTime();
        final BddSet diffPointsTo = node.pointsTo.clone();
        final BddSet prevPointsTo = (BddSet) node.getPrevPointsTo();
        if (diffPointsTo.equals(prevPointsTo)) {
            return;
        }
        diffPointsTo.serialDiffTo(prevPointsTo);
        // note that if some other node has been merge with 'node', the old points-to is a fresh
        // new Bdd set, so the following union does not prevent the reprocessing of all the points-to
        // of 'node' (since it updates a different variable)
        prevPointsTo.unionTo(diffPointsTo);
        long endTime = milliTime();
        Statistics.ruleTimes[Statistics.UPDATE_DIFF_PTS].add((int) (endTime - startTime), MethodFlag.NONE);

        // HCD RULE
        OnlineNode hcdRep = hcd(node, diffPointsTo, worklist);
        startTime = milliTime();
        Statistics.ruleTimes[Statistics.HCD].add((int) (startTime - endTime), MethodFlag.NONE);
        // merged during HCD?
        if (!node.isRep()) {
            return;
        }

        // LOAD RULE
        IntSparseBitVector diffPointsToSet = null;
        if (!node.isNeighborhoodEmpty(Constraint.LOAD)) {
            diffPointsToSet = processLoads(node, hcdRep, diffPointsTo, worklist);
        }
        endTime = milliTime();
        Statistics.ruleTimes[Constraint.LOAD].add((int) (endTime - startTime), MethodFlag.NONE);

        // STORE RULE
        if (!node.isNeighborhoodEmpty(Constraint.STORE)) {
            processStores(node, hcdRep, diffPointsTo, diffPointsToSet, worklist);
        }
        startTime = milliTime();
        Statistics.ruleTimes[Constraint.STORE].add((int) (startTime - endTime), MethodFlag.NONE);
        // when a node is merged by another thread, instead of emptying its information
        // we just lazily wait for the current thread to discover that the node should
        // not be further processed
        if (!node.isRep()) {
            return;
        }

        // GEP RULE
        if (!node.isNeighborhoodEmpty(Constraint.GEP)) {
            if (Configuration.USE_BDD_ADDER) {
                processGep(node, diffPointsTo, worklist);
            } else {
                processGepWithoutAdder(node, diffPointsTo, worklist);
            }
        }
        endTime = milliTime();
        Statistics.ruleTimes[Constraint.GEP].add((int) (endTime - startTime), MethodFlag.NONE);

        // COPY RULE
        if (!node.isNeighborhoodEmpty(Constraint.COPY)) {
            propagatePointsTo(node, diffPointsTo, worklist);
        }
        startTime = milliTime();
        Statistics.ruleTimes[Constraint.COPY].add((int) (startTime - endTime), MethodFlag.NONE);
    }

    private OnlineNode hcd(final OnlineNode node, final IntSet diffPointsTo, final ForeachContext<OnlineNode> worklist) {
        //Is there an offline cycle (*node, hv)?
        int hv = hcdTable.get(node.id);
        if (hv > 0) {
            OnlineNode rep = onlineGraph.getNode(hv, FLAG).getRep(onlineGraph, FLAG);
            final MutableReference<OnlineNode> hcdRepRef = new MutableReference<OnlineNode>(rep);
            // if so, merge everything in our pointsTo with hv.
            diffPointsTo.map(new LambdaVoid<Integer>() {
                @Override
                public void call(Integer index) {
                    OnlineNode x = onlineGraph.getNode(index, FLAG).getRep(onlineGraph, FLAG);
                    if (x.id != OnlineNode.I2P) {
                        OnlineNode hcdRep = hcdRepRef.get();
                        hcdRep = hcdRep.merge(x, true, onlineGraph, FLAG);
                        hcdRepRef.set(hcdRep);
                    }
                }
            });
            OnlineNode hcdRep = hcdRepRef.get();
            if (hcdRep.id != 0 && hcdRep.addToWorklist()) {
                worklist.add(hcdRep, MethodFlag.NONE);
            }
            return hcdRep;
        }
        return null;
    }

    //  DST = *NODE
    private IntSparseBitVector processLoads(final OnlineNode node, final OnlineNode hcdRep, final IntSet diffPointsTo,
                                            final ForeachContext<OnlineNode> worklist) {
        final IntSparseBitVector seen = new IntSparseBitVector();
        final MutableReference<IntSparseBitVector> diffPointsToSet = new MutableReference<IntSparseBitVector>(null);
        final Lambda4Void<Integer, OnlineNode, IntSparseBitVector, MutableBoolean> closure = new Lambda4Void<Integer, OnlineNode, IntSparseBitVector, MutableBoolean>() {
            @Override
            public void call(Integer index, OnlineNode dst, IntSparseBitVector newDiffPointsToSet, MutableBoolean modified) {
                OnlineNode elem = onlineGraph.getNode(index, FLAG).getRep(onlineGraph, FLAG);
                if (newDiffPointsToSet.add(elem.id)) {
                    // if we already saw the pair (elem, dst), ignore.
                    boolean curr = elem.addCopyEdgePropagatePoints2(dst, onlineGraph, FLAG) != null || modified.get();
                    modified.set(curr);
                }
            }
        };
        onlineGraph.map(node, new LambdaVoid<OnlineNode>() {
            @Override
            public void call(OnlineNode s) {
                final OnlineNode dst = s.getRep(onlineGraph, FLAG);
                // the representatives might have changed in between, but let's give it a try
                if (!seen.add(dst.id)) {
                    return;
                }
                if (hcdRep != null) {
                    // skip the points-to iteration: add HCD_REP -> DEST
                    OnlineNode dstRep = hcdRep.addCopyEdgePropagatePoints2(dst, onlineGraph, FLAG);
                    if (dstRep != null && dstRep.addToWorklist()) {
                        worklist.add(dstRep, MethodFlag.NONE);
                    }
                    return;
                }
                final MutableBoolean modified = new MutableBoolean(false);
                final IntSparseBitVector newDiffPointsToSet = new IntSparseBitVector();
                IntSet intSet = diffPointsToSet.get() == null ? diffPointsTo : diffPointsToSet.get();
                intSet.map(closure, dst, newDiffPointsToSet, modified);
                diffPointsToSet.set(newDiffPointsToSet);
                if (modified.get() && dst.addToWorklist()) {
                    worklist.add(dst, MethodFlag.NONE);
                }
            }
        }, Constraint.LOAD, FLAG);
        return diffPointsToSet.get();
    }

    // *NODE = SRC
    private void processStores(final OnlineNode node, final OnlineNode hcdRep, final IntSet diffPointsTo,
                               final IntSparseBitVector diffPointsToSet, final ForeachContext<OnlineNode> worklist) {
        final IntSparseBitVector seen = new IntSparseBitVector();
        final Lambda3Void<Integer, OnlineNode, IntSparseBitVector> closure = new Lambda3Void<Integer, OnlineNode, IntSparseBitVector>() {
            @Override
            public void call(Integer index, OnlineNode src, IntSparseBitVector dstSeen) {
                OnlineNode elem = onlineGraph.getNode(index, FLAG).getRep(onlineGraph, FLAG);
                if (!dstSeen.add(elem.id)) {
                    return;
                }
                OnlineNode elemRep = src.addCopyEdgePropagatePoints2(elem, onlineGraph, FLAG);
                //Statistics.storeRules.incrementAndGet();
                if (elemRep != null && elemRep.addToWorklist()) {
                    worklist.add(elemRep, MethodFlag.NONE);
                }
            }
        };
        onlineGraph.map(node, new LambdaVoid<OnlineNode>() {
            @Override
            public void call(OnlineNode s) {
                final OnlineNode src = s.getRep(onlineGraph, FLAG);
                if (!seen.add(src.id)) {
                    return;
                }
                if (hcdRep != null) {
                    // skip the points-to iteration: add SRC -> HCD_REP
                    OnlineNode hcdRepNode = src.addCopyEdgePropagatePoints2(hcdRep, onlineGraph, FLAG);
                    if (hcdRepNode != null && hcdRepNode.addToWorklist()) {
                        worklist.add(hcdRepNode, MethodFlag.NONE);
                    }
                    return;
                }
                final IntSparseBitVector dstSeen = new IntSparseBitVector();
                IntSet intSet = diffPointsToSet == null ? diffPointsTo : diffPointsToSet;
                intSet.map(closure, src, dstSeen);
            }
        }, Constraint.STORE, FLAG);
    }

    // DST = SRC + OFFSET
    private void processGep(final OnlineNode node, final IntSet diffPointsTo, final ForeachContext<OnlineNode> worklist) {
        BddDomain pointsToDomain = BddDomain.getDomain(0);
        final BddNode ptsVar = pointsToDomain.set();
        final LongSparseBitVector seen = new LongSparseBitVector();
        onlineGraph.map(node, new Lambda2Void<Integer, OnlineNode>() {
            @Override
            public void call(Integer offset, OnlineNode d) {
                OnlineNode dst = d.getRep(onlineGraph, FLAG);
                long key = ((long) dst.id << 32) | offset;
                if (!seen.add(key)) {
                    return;
                }
                // for every element x in the points-to of SRC (node), add (x+offset) to the points-to of DST
                BddNode tmp1 = Bdd.relProd(((BddSet) diffPointsTo).getRoot(), geps[offset], ptsVar);
                if (Bdd.isEmpty(tmp1)) {
                    return;
                }
                BddSet filtered = new BddSet(Bdd.replace(tmp1, gep2pts));
                dst = dst.propagatePointsTo(filtered, onlineGraph, FLAG);
                if (dst != null && dst.addToWorklist()) {
                    worklist.add(dst, MethodFlag.NONE);
                }
            }
        }, Constraint.GEP, FLAG);
    }

    void processGepWithoutAdder(final OnlineNode node, final BddSet diffPointsTo, final ForeachContext<OnlineNode> worklist) {
        final LongSparseBitVector seen = new LongSparseBitVector();
        final TIntArrayList offsets = new TIntArrayList(4);
        final ArrayList<BddSet> results = new ArrayList<BddSet>(4);
        final TIntArrayList elements = diffPointsTo.elements();
        onlineGraph.map(node, new Lambda2Void<Integer, OnlineNode>() {
            @Override
            public void call(final Integer offset, OnlineNode d) {
                OnlineNode dst = d.getRep(onlineGraph, FLAG);
                long key = ((long) dst.id << 32) | offset;
                if (!seen.add(key)) {
                    return;
                }
                int cacheIndex = offsets.indexOf(offset);
                BddSet shiftedDiffPointsTo;
                if (cacheIndex != -1) {
                    shiftedDiffPointsTo = results.get(cacheIndex);
                } else {
                    shiftedDiffPointsTo = new BddSet();
                    for (int i = 0; i < elements.size(); i++) {
                        int index = elements.getQuick(i);
                        final OnlineNode node1 = onlineGraph.getNode(index, FLAG);
                        if (node1.getObjSize() > offset) {
                            shiftedDiffPointsTo.add(index + offset);
                        }
                    }
                    offsets.add(offset);
                    results.add(shiftedDiffPointsTo);
                }
                if (shiftedDiffPointsTo.isEmpty()) {
                    return;
                }
                dst = dst.propagatePointsTo(shiftedDiffPointsTo, onlineGraph, FLAG);
                if (dst != null && dst.addToWorklist()) {
                    worklist.add(dst, MethodFlag.NONE);
                }
            }
        }, Constraint.GEP, FLAG);
    }

    private void propagatePointsTo(final OnlineNode node, final IntSet diffPointsTo, final ForeachContext<OnlineNode> worklist) {
        final IntSparseBitVector seen = new IntSparseBitVector();
        onlineGraph.map(node, new LambdaVoid<OnlineNode>() {
            @Override
            public void call(OnlineNode s) {
                OnlineNode dst = s.getRep(onlineGraph, FLAG);
                if (node == dst || !seen.add(dst.id)) {
                    return;
                }
                dst = dst.propagatePointsTo(diffPointsTo, onlineGraph, FLAG);
                if (dst != null && dst.addToWorklist()) {
                    worklist.add(dst, MethodFlag.NONE);
                }
            }
        }, Constraint.COPY, FLAG);
    }

    // debug method
    static Set<OnlineNode> findIncomingCopyNeighbors(final MultiGraph<OnlineNode> onlineGraph, final OnlineNode src) {
        return findIncomingNeighbors(onlineGraph, src, Constraint.COPY);
    }

    static Set<OnlineNode> findIncomingCopyAndGepNeighbors(final MultiGraph<OnlineNode> onlineGraph, final OnlineNode src) {
        Set<OnlineNode> ret = findIncomingNeighbors(onlineGraph, src, Constraint.COPY);
        ret.addAll(findIncomingGepNeighbors(onlineGraph, src));
        return ret;
    }

    static Set<OnlineNode> findIncomingNeighbors(final MultiGraph<OnlineNode> onlineGraph, final OnlineNode src, final byte domain) {
        final HashSet<OnlineNode> res = new HashSet<OnlineNode>();
        onlineGraph.map(new LambdaVoid<OnlineNode>() {
            @Override
            public void call(final OnlineNode node) {
                onlineGraph.map(node, new LambdaVoid<OnlineNode>() {
                    @Override
                    public void call(OnlineNode dst) {
                        if (dst == src) {
                            res.add(node);
                        }
                    }
                }, domain, NONE);
            }
        });
        return res;
    }

    static Set<OnlineNode> findIncomingGepNeighbors(final MultiGraph<OnlineNode> onlineGraph, final OnlineNode src) {
        final HashSet<OnlineNode> res = new HashSet<OnlineNode>();
        onlineGraph.map(new LambdaVoid<OnlineNode>() {
            @Override
            public void call(final OnlineNode node) {
                onlineGraph.map(node, new Lambda2Void<Integer, OnlineNode>() {
                    @Override
                    public void call(Integer _, OnlineNode dst) {
                        if (dst == src) {
                            res.add(node);
                        }
                    }
                }, Constraint.GEP, NONE);
            }
        });
        return res;
    }

    // debug method
    static Set<OnlineNode> findRepresentedBy(final MultiGraph<OnlineNode> onlineGraph, final OnlineNode src) {
        if (!src.isRep()) {
            throw new RuntimeException();
        }
        final HashSet<OnlineNode> res = new HashSet<OnlineNode>();
        onlineGraph.map(new LambdaVoid<OnlineNode>() {
            @Override
            public void call(OnlineNode dst) {
                if (dst.getRep(onlineGraph, NONE) == src) {
                    res.add(dst);
                }
            }
        });
        return res;
    }

    // print the copy and GEP neighbors of a node.
    // this set might be affected by a change in the points-to of the node (while load/store neighbors are not).
    private static Set<OnlineNode> findOutgoingCopyAndGepNeighbors(final MultiGraph<OnlineNode> onlineGraph, OnlineNode nodeRep) {
        final Set<OnlineNode> res = findOutgoingNeighbors(onlineGraph, nodeRep, Constraint.COPY);
        res.addAll(findOutgoingGepNeighbors(onlineGraph, nodeRep));
        return res;
    }

    private static Set<OnlineNode> findOutgoingNeighbors(final MultiGraph<OnlineNode> onlineGraph, OnlineNode nodeRep, byte domain) {
        final HashSet<OnlineNode> res = new HashSet<OnlineNode>();
        onlineGraph.map(nodeRep, new LambdaVoid<OnlineNode>() {
            @Override
            public void call(OnlineNode d) {
                OnlineNode dst = d.getRep(onlineGraph, MethodFlag.NONE);
                res.add(dst);
            }
        }, domain, MethodFlag.NONE);
        return res;
    }


    private static Set<OnlineNode> findOutgoingGepNeighbors(final MultiGraph<OnlineNode> onlineGraph, OnlineNode nodeRep) {
        final HashSet<OnlineNode> res = new HashSet<OnlineNode>();
        onlineGraph.map(nodeRep, new Lambda2Void<Integer, OnlineNode>() {
            @Override
            public void call(Integer _, OnlineNode d) {
                OnlineNode dst = d.getRep(onlineGraph, MethodFlag.NONE);
                res.add(dst);
            }
        }, Constraint.GEP, MethodFlag.NONE);
        return res;
    }

    private static final class Collection2ForEachContextAdapter<T> extends AbstractForeachContext<T> {
        private final ConcurrentGrowingList<T> list;

        Collection2ForEachContextAdapter(int maxSize) {
            list = new ConcurrentGrowingList<T>(maxSize);
        }

        List<T> get() {
            return list;
        }

        @Override
        public void add(T onlineNode) {
            list.add(onlineNode);
        }

        @Override
        public void add(T onlineNode, byte flags) {
            list.add(onlineNode);
        }
    }
}
