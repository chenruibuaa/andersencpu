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

 File: Statistics.java
 */

package hardekopfPointsTo.main;

import galois.objects.IntegerAccumulator;
import galois.objects.IntegerAccumulatorBuilder;
import galois.objects.MethodFlag;
import util.CollectionMath;
import util.MutableInteger;
import util.fn.FnIterable;
import util.fn.Lambda;
import util.fn.LambdaVoid;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import static galois.objects.MethodFlag.NONE;
import static hardekopfPointsTo.main.Constraint.*;

final class Statistics {

    // input
    static int numNodes;
    public static int numConstraints;
    static final AtomicInteger objectNodes;
    static final AtomicInteger[] initialConstraintCount;
    // offline optimizations
    static int repNodes;
    static int repValNodes;
    static long[] reducedConstraintCount;

    static int nodesMergedInHvn;
    static int hcdSize;
    static int varNodesMergedInHcd;
    static final AtomicInteger varNodesMergedInOnHcd;
    // worklist
    static int nodesPushed;
    // solve phase
    static int hcdOnScc;
    static int hcdOnSccNodes;
    static int passes;

    static IntegerAccumulator nodeRuns;
    static int copyEdgesDeleted;
    static int complexConstraintsDeleted;

    static final IntegerAccumulator[] ruleTimes;
    public static final byte HCD = 0;
    public static final byte UPDATE_DIFF_PTS = 5;
    public static final byte CREATE_GRAPH = 6;
    public static final AtomicInteger storeRules;

    /// runtime statistics
    enum Phase {
        READ_INPUT, OFFLINE_SEQ, OFFLINE, ONLINE_SEQ, ONLINE, VERIFY;

        ArrayList<Integer> times = new ArrayList<Integer>(6);
    }

    static {
        objectNodes = new AtomicInteger(0);
        varNodesMergedInOnHcd = new AtomicInteger(0);
        initialConstraintCount = new AtomicInteger[5];
        for (int i = 0; i < initialConstraintCount.length; i++) {
            initialConstraintCount[i] = new AtomicInteger(0);
        }
        reducedConstraintCount = new long[6];
        ruleTimes = new IntegerAccumulator[7]; //create, update, hcd, copy, load, store, gep
        for (int i = 0; i < ruleTimes.length; i++) {
            ruleTimes[i] = new IntegerAccumulatorBuilder().create(0);
        }
        storeRules = new AtomicInteger(0);
    }

    static void reset() {
        numNodes = 0;
        numConstraints = 0;
        objectNodes.set(0);
        for (AtomicInteger ai : initialConstraintCount) {
            ai.set(0);
        }
        nodesMergedInHvn = 0;
        varNodesMergedInHcd = 0;
        varNodesMergedInOnHcd.set(0);
        hcdSize = 0;
        copyEdgesDeleted = 0;
        Arrays.fill(reducedConstraintCount, 0);
        repValNodes = 0;
        passes = 0;
        nodeRuns = new IntegerAccumulatorBuilder().create(0);
        complexConstraintsDeleted = 0;
        hcdOnScc = 0;
        hcdOnSccNodes = 0;
        nodesPushed = 0;
        for (IntegerAccumulator integerAccumulator : ruleTimes) {
            integerAccumulator.set(0);
        }
        storeRules.set(0);
    }

    static void addTime(Phase phase, long time) {
        phase.times.add((int) time);
    }

    static void accTime(Phase phase, long time) {
        final int last = phase.times.size() - 1;
        int curr = phase.times.get(last);
        phase.times.set(last, curr + (int) time);
    }

    static void resetTimes() {
        for (Phase phase : Phase.values()) {
            phase.times.clear();
        }
    }

    public static void printStats(Logger logger, final MultiGraph<OnlineNode> onlineGraph) {
        if (!logger.isLoggable(Level.INFO)) {
            return;
        }
        logger.info("==========  Statistics  ====================================");
        logger.info("Initial value nodes..................... " + (numNodes - OnlineNode.FIRST_VAR_NODE - objectNodes.get()));
        logger.info("- object nodes.......................... " + objectNodes.get());
        logger.info("Initial constraints..................... " + numConstraints);
        logger.info("- addr_of............................... " + initialConstraintCount[ADDR_OF]);
        logger.info("- copy.................................. " + initialConstraintCount[COPY]);
        logger.info("- load.................................. " + initialConstraintCount[LOAD]);
        logger.info("- store................................. " + initialConstraintCount[STORE]);
        logger.info("- GEP................................... " + initialConstraintCount[GEP]);
        logger.info("Remaining (rep) value nodes............. " + repValNodes);
        logger.info("Reduced constraints..................... " + reducedConstraintCount[5]);
        logger.info("- addr_of............................... " + reducedConstraintCount[ADDR_OF]);
        logger.info("- copy.................................. " + reducedConstraintCount[COPY]);
        logger.info("- load.................................. " + reducedConstraintCount[LOAD]);
        logger.info("- store................................. " + reducedConstraintCount[STORE]);
        logger.info("- GEP................................... " + reducedConstraintCount[GEP]);
        logger.info("Nodes merged in HVN..................... " + nodesMergedInHvn);
        logger.info("HCD map entries......................... " + hcdSize);
        logger.info("- VARs merged offline................... " + varNodesMergedInHcd);
        logger.info("- VARs merged online.................... " + varNodesMergedInOnHcd);
        logger.info("- SCCs detected online.................. " + hcdOnScc);
        logger.info("  - nodes in these...................... " + hcdOnSccNodes);
        logger.info("Solver passes........................... " + "NA");
        logger.info("- solve_node runs....................... " + nodeRuns.get());
        logger.info("- store rule runs........................" + storeRules.get());
        //printMemoryUsage(logger, onlineGraph);
        logger.info("");
    }

    private static void printGraphStats(Logger logger, final MultiGraph<OnlineNode> onlineGraph) {
        int size = onlineGraph.size();
        final MutableInteger pts = new MutableInteger(0);
        final MutableInteger copy = new MutableInteger(0);
        final MutableInteger load = new MutableInteger(0);
        final MutableInteger store = new MutableInteger(0);
        final MutableInteger gep = new MutableInteger(0);
        final MutableInteger maxPts = new MutableInteger(Integer.MIN_VALUE);
        final MutableInteger maxCopy = new MutableInteger(Integer.MIN_VALUE);
        final MutableInteger maxLoad = new MutableInteger(Integer.MIN_VALUE);
        final MutableInteger maxStore = new MutableInteger(Integer.MIN_VALUE);
        final MutableInteger maxGep = new MutableInteger(Integer.MIN_VALUE);

        final MutableInteger varsWithStoreEdges = new MutableInteger(0);

        onlineGraph.map(new LambdaVoid<OnlineNode>() {
            @Override
            public void call(OnlineNode _) {
                final OnlineNode node = _.getRep(onlineGraph, MethodFlag.NONE);
                final int p = node.pointsTo.size();
                pts.add(p);
                maxPts.set(Math.max(maxPts.get(), p));
                final int c = node.neighborsSize(Constraint.COPY);
                copy.add(c);
                maxCopy.set(Math.max(maxCopy.get(), c));
                final int l = node.neighborsSize(Constraint.LOAD);
                load.add(l);
                maxLoad.set(Math.max(maxLoad.get(), l));
                final int s = node.neighborsSize(Constraint.STORE);
                if (s > 0) {
                    varsWithStoreEdges.incrementAndGet();
                }
                store.add(s);
                maxStore.set(Math.max(maxStore.get(), s));
                final int g = node.neighborsSize(Constraint.GEP);
                gep.add(g);
                maxGep.set(Math.max(maxGep.get(), g));
            }
        });
        logger.info("Number of nodes: " + size);
        logger.info("Edges (total/max)");
        logger.info("- pts-to................................ " + pts + " (" + maxPts + ")");
        logger.info("- copy.................................. " + copy + " (" + maxCopy + ")");
        logger.info("- load.................................. " + load + " (" + maxLoad + ")");
        logger.info("- store................................. " + store + " (" + maxStore + ")");
        logger.info("- gep................................... " + gep + " (" + maxGep + ")");
        logger.info("");
        logger.info("% vars with store edges................. " + 100f * (float) (varsWithStoreEdges.get()) / (float) size);
    }

    private static int countRepresentedBy(final MultiGraph<OnlineNode> onlineGraph, final OnlineNode node) {
        final MutableInteger res = new MutableInteger(0);
        onlineGraph.map(new LambdaVoid<OnlineNode>() {
            @Override
            public void call(OnlineNode dst) {
                if (dst.getRep(onlineGraph, NONE) == node) {
                    res.add(1);
                }
            }
        });
        return res.get();
    }

    protected static void printSummary(Logger logger) {
        logger.info("\nSummary of the runs:");
        logger.fine(summarizeInts(Phase.READ_INPUT.times, "read input: "));
        logger.info(summarizeInts(Phase.OFFLINE.times, "offline: "));
        logger.info(summarizeInts(diff(Phase.OFFLINE.times, Phase.OFFLINE_SEQ.times), "    par: "));
        logger.info(summarizeInts(Phase.OFFLINE_SEQ.times, "    seq: "));
        logger.info(summarizeInts(Phase.ONLINE.times, "solve: "));
        logger.info(summarizeInts(diff(Phase.ONLINE.times, Phase.ONLINE_SEQ.times), "    par: "));
        logger.info(summarizeInts(Phase.ONLINE_SEQ.times, "    seq: "));

        if (!Phase.VERIFY.times.isEmpty()) {
            logger.fine(summarizeInts(Phase.VERIFY.times, "verify: "));
        }
    }

    private static Collection<Integer> diff(Collection<Integer> c1, Collection<Integer> c2) {
        int size = c1.size();
        if (c2.size() != size) {
            throw new RuntimeException();
        }
        ArrayList<Integer> ret = new ArrayList<Integer>(size);
        Iterator<Integer> iterator = c2.iterator();
        for (int i1 : c1) {
            ret.add(i1 - iterator.next());
        }
        return ret;
    }

    protected static String summarizeInts(Collection<Integer> list, String prefix) {
        float[] stats = summarizeInts(list, 1);
        if (stats == null) {
            return "";
        }
        return String.format("%smean: %.0f min: %.0f max: %.0f sample stdev: %.2f", prefix, stats[0], stats[1], stats[2], Math
                .sqrt(stats[3]));
    }

    protected static float[] summarizeInts(Collection<Integer> list, int drop) {
        if (list.isEmpty()) {
            return null;
        }
        if (list.size() <= 1 + drop) {
            drop = 0;
        }
        List<Integer> retain = new ArrayList<Integer>(list).subList(drop, list.size());
        final float mean = CollectionMath.sumInteger(retain) / (float) retain.size();
        int min = Collections.min(retain);
        int max = Collections.max(retain);

        float var = CollectionMath.sumFloat(FnIterable.from(retain).map(new Lambda<Integer, Float>() {
            @Override
            public Float call(Integer x) {
                return (x - mean) * (x - mean);
            }
        })) / (retain.size() - 1);
        return new float[]{mean, min, max, var, retain.size()};
    }
}
