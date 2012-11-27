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

 File: Main.java
 */

package hardekopfPointsTo.main;

import com.google.common.collect.ArrayListMultimap;
import galois.objects.Mappables;
import galois.objects.MethodFlag;
import galois.runtime.GaloisRuntime;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.procedure.TIntIntProcedure;
import gnu.trove.set.hash.TIntHashSet;
import util.InputOutput;
import util.Launcher;
import util.MutableInteger;
import util.ThreadTimer;
import util.fn.LambdaVoid;
import util.ints.IntPair;
import util.ints.IntSet;
import util.ints.bdd.BddDomain;
import util.ints.bdd.BddSet;

import java.io.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

public class Main {
    public static final Logger LOGGER = Logger.getLogger("test.hardekopfPointsTo");

    public static void main(String[] args) throws Exception {
        run(args);
    }

    public static void run(String[] args) throws Exception {
        Launcher launcher = Launcher.getLauncher();
        BddDomain.reset();
        Statistics.reset();
        final String benchmarkName = args[0].substring(args[0].lastIndexOf('/') + 1, args[0].indexOf('.'));
        System.err.println();
        if (launcher.isFirstRun()) {
            System.err.println("Hardekopf's points-to analysis (parallel version)");
            System.err.println("number of threads: " + GaloisRuntime.getRuntime().getMaxThreads());
            System.err.println("input: " + benchmarkName.substring(0, benchmarkName.indexOf("_")));
        }
        if (args.length < 2) {
            System.err.println("Usage: <node_file> <contraint_file>");
        }
        if (launcher.isFirstRun()) {
            LOGGER.info("analysis started with config: " + Configuration.printConfiguration());
        }
        String inputDir = new File(args[0]).getParent();
        // phase 0 : load constraints and nodes from file
        ThreadTimer.Tick readStart = ThreadTimer.tick();
        final MultiGraph<OnlineNode> onlineGraph = readGraph(args[0]);
        Constraint.readConstraints(args[1], onlineGraph.size());
        ThreadTimer.Tick end = ThreadTimer.tick();
        long readTime = readStart.elapsedTime(true, end);
        Statistics.addTime(Statistics.Phase.READ_INPUT, readTime);
        LOGGER.fine("runtime for obj_cons_id: " + readTime + " ms (incl. GC: " + readStart.elapsedTime(false, end) + " ms)");
        printConstraints(benchmarkName);
        // phase 1 : offline constraint optimization
        launcher.startTiming();
        TIntIntHashMap hcdTable = new TIntIntHashMap();
        OfflineOptimizer offlineOptimizer = new OfflineOptimizer(onlineGraph, hcdTable);
        offlineOptimizer.constraintOptimization();
        verifyOfflinePhase(hcdTable);
        printHcdInfo(benchmarkName, onlineGraph, hcdTable);
        // phase 2: Bdd initialization and (depending on which version) Gep preprocessing
        Andersen andersen = new Andersen(onlineGraph, hcdTable);
        // phase 3: solve
        andersen.analyze();
        launcher.stopTiming();
        //printFinalStats(onlineGraph);
        printSolution(benchmarkName, onlineGraph);
        if (launcher.isFirstRun()) {
            Statistics.printStats(LOGGER, onlineGraph);
        }
        // verification
        verify(onlineGraph, inputDir, benchmarkName);
        if (launcher.isLastRun()) {
            Statistics.printSummary(LOGGER);
        }
    }

    public static MultiGraph<OnlineNode> readGraph(String filename) throws Exception {
        BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(filename))));
        String strLine = br.readLine();
        int numNodes = Integer.parseInt(strLine);
        OnlineNode[] nodes = new OnlineNode[numNodes];
        final MultiGraph<OnlineNode> ret = new MultiGraph<OnlineNode>(nodes);
        Statistics.numNodes = numNodes;
        OnlineNode.lastObjectNode = Integer.parseInt(br.readLine());
        final int lastFunctionNode = Integer.parseInt(br.readLine());
        GaloisRuntime.forall(Mappables.fromReader(br), new LambdaVoid<String>() {
            @Override
            public void call(String line) {
                String[] info = line.split(",");
                int id = Integer.parseInt(info[0]);
                int obj_sz = Integer.parseInt(info[1]);
                boolean functionObject = Integer.parseInt(info[2]) == 1;
                if (id >= OnlineNode.FIRST_VAR_NODE && obj_sz > 0) {
                    Statistics.objectNodes.incrementAndGet();
                }
                if (ret.getNode(id, MethodFlag.NONE) != null) {
                    throw new RuntimeException();
                }
                OnlineNode node = new OnlineNode(id, obj_sz);
                node.setFunction(functionObject && id <= lastFunctionNode);
                ret.setNode(id, node, MethodFlag.NONE);
            }
        });
        return ret;
    }

    private static void printFinalStats(final MultiGraph<OnlineNode> onlineGraph) {
        final MutableInteger copyEdges = new MutableInteger(0);
        final MutableInteger ptsEdges = new MutableInteger(0);
        onlineGraph.map(new LambdaVoid<OnlineNode>() {
            @Override
            public void call(OnlineNode node) {
                copyEdges.add(node.copy.size());
                ptsEdges.add(node.pointsTo.size());
            }
        });
        System.out.println("Final #PTS edges: " + ptsEdges);
        System.out.println("Final #COPY edges: " + copyEdges);
    }


    private static void printConstraints(String benchmarkName) throws Exception {
        final boolean SORT = false;
        if (!Configuration.PRINT_CONSTRAINTS) {
            return;
        }
        final Collection<PrintedConstraint> constraints[] = (Collection<PrintedConstraint>[]) new Collection[Constraint.GEP + 1];
        for (int i = 0; i <= Constraint.GEP; i++) {
            constraints[i] = SORT ? new TreeSet<PrintedConstraint>() : new ArrayList<PrintedConstraint>();
        }
        int numConstraints = Constraint.constraints.length;

        GaloisRuntime.forall(Mappables.range(0, numConstraints, Configuration.getSequentialChunkSize(numConstraints)), new LambdaVoid<Integer>() {
            @Override
            public void call(Integer index) {
                Constraint constraint = Constraint.constraints[index];
                if (SORT && (constraint == null || (constraint.type != Constraint.GEP && constraint.offset > 0))) {
                    return;
                }
                constraints[constraint.type].add(new PrintedConstraint(constraint, index));
            }
        });

        String outputFileName = (benchmarkName.substring(0, benchmarkName.indexOf("_"))) + "_constraints.txt";
        System.err.println("Printing constraints in " + outputFileName);
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(outputFileName));
        PrintStream output = new PrintStream(bufferedOutputStream);
        output.println("# number of constraints of each type (address-of, copy, load, store, gep)");
        output.println(constraints[Constraint.ADDR_OF].size() + "," + constraints[Constraint.COPY].size() + "," +
                constraints[Constraint.LOAD].size() + "," + constraints[Constraint.STORE].size() + "," + constraints[Constraint.GEP].size());
        output.println();
        printConstraints("ADDRESS-OF", "# sorted by <dst, src>", constraints[Constraint.ADDR_OF], output);
        printConstraints("COPY", "# sorted by <dst, src>", constraints[Constraint.COPY], output);
        printConstraints("LOAD", "# sorted by <dst, src>", constraints[Constraint.LOAD], output);
        printConstraints("STORE", "# sorted by <src, dst> (src and dst are flipped)", constraints[Constraint.STORE], output);
        printConstraints("GEP", "# sorted by <dst, src>", constraints[Constraint.GEP], output);
        bufferedOutputStream.close();
        output.close();
        System.err.println("The constraints file size is " + new File(outputFileName).length() + " bytes");
        //System.err.println("Constraints printed, exiting...");
        //System.exit(0);
    }

    private static void printConstraints(String type, String sortedMsg, Collection<PrintedConstraint> list, PrintStream output) {
        output.println("# " + type + " constraints");
        output.println("# format: id, src, dst, type, offset");
        output.println(sortedMsg);
        for (PrintedConstraint printedConstraint : list) {
            output.println(printedConstraint);
        }
        output.println();
    }

    // an extension of the constraint class created exclusively for printing the constraints in a specific format
    private static class PrintedConstraint extends Constraint implements Comparable<PrintedConstraint> {
        int id;

        PrintedConstraint(Constraint constraint, int id) {
            super(constraint.src, constraint.dst, constraint.offset, constraint.type);
            this.id = id;
        }

        @Override
        public int compareTo(PrintedConstraint constraint) {
            if (type != constraint.type) {
                return type - constraint.type;
            }
            return dst == constraint.dst ? src - constraint.src : dst - constraint.dst;
        }

        @Override
        public String toString() {
            return id + "," + src + "," + dst + "," + type + "," + offset;
        }
    }

    private static void printHcdInfo(String benchmarkName, final MultiGraph<OnlineNode> onlineGraph, TIntIntHashMap hcdTable) throws IOException {
        if (!Configuration.PRINT_HCD_TABLE || !Configuration.USE_HCD) {
            return;
        }
        String outputFileName = benchmarkName.substring(0, benchmarkName.indexOf("_")) + "_hcd.txt";
        System.err.println("Printing HCD table of size " + hcdTable.size() + " in " + outputFileName);
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(outputFileName));
        final PrintStream output = new PrintStream(bufferedOutputStream);
        // a) first step: print representatives found so far
        // we will print a1) number of merged nodes a2) list of (node, rep) sorted by 'node'
        final StringBuffer sb = new StringBuffer();
        final MutableInteger merges = new MutableInteger(0);
        onlineGraph.map(new LambdaVoid<OnlineNode>() {
            @Override
            public void call(OnlineNode node) {
                if (!node.isRep()) {
                    sb.append(node.id + "," + node.getRep(onlineGraph, MethodFlag.NONE).id + "\n");
                    merges.incrementAndGet();
                }
            }
        });
        output.println("# first row: number of offline HCD merges. <y,x> means rep(y) = x");
        output.println(merges.get());
        output.print(sb);
        // b) print HCD table
        final List<IntPair> sorted = new ArrayList<IntPair>();
        final HashSet<Integer> seen = new HashSet<Integer>();
        hcdTable.forEachEntry(new TIntIntProcedure() {
            @Override
            public boolean execute(int a, int b) {
                sorted.add(new IntPair(b, a));
                seen.add(b);
                return true;
            }
        });
        Collections.sort(sorted);
        output.println("# <y,x> means that *x is PTS-equivalent to y. 1st row: number of different 'y', number of pairs");
        output.println(seen.size() + "," + hcdTable.size());
        for (IntPair intPair : sorted) {
            output.println(intPair.first + "," + intPair.second);
        }
        bufferedOutputStream.close();
        output.close();
        System.exit(0);
    }

    private static void printSolution(String benchmarkName, MultiGraph<OnlineNode> onlineGraph) throws IOException {
        if (Configuration.PRINT_SOLUTION || Configuration.PRINT_SOLUTION_GRAPH) {
            String outputFileName = (benchmarkName.substring(0, benchmarkName.indexOf("_"))) + "_correct_soln" + getFileSuffix() + ".txt";
            System.err.println("Printing solution in " + outputFileName);
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(outputFileName));
            PrintStream output = new PrintStream(bufferedOutputStream);
            if (Configuration.PRINT_SOLUTION_GRAPH) {
                printDatGraph(output, onlineGraph);
                //printCycleInfo(output, onlineGraph);
            } else {
                printPointsTo(output, onlineGraph);
            }
            bufferedOutputStream.close();
            output.close();
            System.err.println("The solution file size is " + new File(outputFileName).length() + " bytes");
        }
    }

    private static void printCycleInfo(final PrintStream ps, final MultiGraph<OnlineNode> onlineGraph) {
        final int[] histogram = new int[onlineGraph.size()];
        onlineGraph.map(new LambdaVoid<OnlineNode>() {
            @Override
            public void call(final OnlineNode node) {
                histogram[node.getRep(onlineGraph, MethodFlag.NONE).id]++;
            }
        });
        boolean first = true;
        Arrays.sort(histogram);
        for (int aHistogram : histogram) {
            if (aHistogram > 1) {
                if (first) {
                    ps.print(aHistogram);
                    first = false;
                } else {
                    ps.print("," + aHistogram);
                }
            }
        }
    }

    // print final points-to graph in Matlab format
    private static void printDatGraph(final PrintStream ps, final MultiGraph<OnlineNode> onlineGraph) {
        onlineGraph.map(new LambdaVoid<OnlineNode>() {
            @Override
            public void call(final OnlineNode node) {
                IntSet pointsTo = node.getRep(onlineGraph, MethodFlag.NONE).pointsTo;
                pointsTo.map(new LambdaVoid<Integer>() {
                    @Override
                    public void call(Integer dst) {
                        ps.println(node.id + "    " + dst + "    1");
                    }
                });
            }
        });
    }

    private static void printPointsTo(PrintStream ps, final MultiGraph<OnlineNode> onlineGraph) {
        ps.println(onlineGraph.size());
        final ArrayListMultimap<IntSet, Integer> pointsTo = ArrayListMultimap.create();
        onlineGraph.map(new LambdaVoid<OnlineNode>() {
            @Override
            public void call(OnlineNode node) {
                pointsTo.put(node.getRep(onlineGraph, MethodFlag.NONE).pointsTo, node.id);
            }
        });
        for (IntSet dest : pointsTo.keySet()) {
            Collection<Integer> src = pointsTo.get(dest);
            ps.println(src + " => " + dest);
        }
    }

    private static void verifyOfflinePhase(TIntIntHashMap hcdTable) {
        if (!verifyOfflinePhase()) {
            return;
        }
        System.err.print("Verifying offline phase...");
        // the following verification could pass and the results still be wrong. For instance, consider the situation where
        // in a correct execution we merge variables (x1,x2), which only appear in the copy constraint (x1,x2).
        // if instead (due to a bug), we merge variables (y1,y2) which only appear in a copy constraint (y1,y2), the
        // number of merges and the final number of constraints/representatives is the same => the bug goes unnoticed.
        // But doing a comprehensive check is too time-consuming, so we will use the statistics as a "weak proof" of
        // correctness.
        checkProperty("galois.hardekopfPointsTo.nodesMergedInHvn", Statistics.nodesMergedInHvn, "Incorrect number of nodes merged during HVN/HRU. ");
        checkProperty("galois.hardekopfPointsTo.repNodes", Statistics.repNodes, "Incorrect number of representative nodes. ");
        checkProperty("galois.hardekopfPointsTo.reducedConstraintCount", (int) Statistics.reducedConstraintCount[5], "Incorrect number of reduced constraints. ");
        checkProperty("galois.hardekopfPointsTo.hcdTableSize", hcdTable.size(), "Incorrect size of HCD table.");
        System.err.println("OK");
    }

    public static boolean verifyOfflinePhase() {
        if (Configuration.PRINT_SOLUTION || Configuration.VERIFY_OFFLINE == 0 || (!Launcher.getLauncher().isFirstRun() && Configuration.VERIFY_RESULT == 1)) {
            return false;
        }
        // if some offline phase has been disabled, the results will not match -  verification has to be disabled too
        boolean ret = Configuration.USE_HCD && Configuration.USE_HVN;
        if (!ret && Launcher.getLauncher().isFirstRun()) {
            LOGGER.warning("warning: offline verification has been deactivated because HVN or HCD are not being used.");
        }
        return ret;
    }

    private static void checkProperty(String property, int found, String errorMsgPrefix) {
        Integer expected = Integer.getInteger(property);
        if (expected == null) {
            LOGGER.warning("warning: offline verification is active, but property: " + property + " is undefined.");
        } else if (found != expected) {
            final String errorMsg = errorMsgPrefix + " Expected: " + expected + ", found: " + found;
            throw new IllegalStateException(errorMsg);
        }
    }

    public static void verify(MultiGraph<OnlineNode> onlineGraph, String inputDir, String benchmarkName) throws Exception {
        if (!verify()) {
            return;
        }
        ThreadTimer.Tick start = ThreadTimer.tick();
        verify_(onlineGraph, inputDir, benchmarkName);
        ThreadTimer.Tick end = ThreadTimer.tick();
        long verifyTime = start.elapsedTime(true, end);
        Statistics.addTime(Statistics.Phase.VERIFY, verifyTime);
        LOGGER.fine("runtime for verify: " + verifyTime + " ms (incl. GC: " + start.elapsedTime(false, end) + " ms)");
    }

    static boolean verify() {
        return !Configuration.PRINT_SOLUTION && (Configuration.VERIFY_RESULT > 1 || (Launcher.getLauncher().isFirstRun() && Configuration.VERIFY_RESULT == 1));
    }

    public static void verify_(final MultiGraph<OnlineNode> onlineGraph, final String inputDir, final String benchmarkName) throws Exception {
        String correctOutputFileName = inputDir + util.InputOutput.FILE_SEPARATOR +
                (benchmarkName.substring(0, benchmarkName.indexOf("_"))) + "_correct_soln";
        correctOutputFileName += getFileSuffix();
        correctOutputFileName += ".txt.gz";
        System.err.print("Verifying solution against " + correctOutputFileName + "...");
        InputStream in = new GZIPInputStream(new FileInputStream(correctOutputFileName));
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String strLine = br.readLine();
        int numNodes = Integer.parseInt(strLine);
        if (numNodes != onlineGraph.size()) {
            throw new IllegalStateException(
                    "The result contains a number of variables different to the one in the correct version: "
                            + numNodes + " != " + onlineGraph.size());
        }
        try {
            GaloisRuntime.forall(Mappables.fromReader(br), new LambdaVoid<String>() {
                @Override
                public void call(String strLine) {
                    int arrowIndex = strLine.indexOf("=>");
                    String lhs = strLine.substring(0, arrowIndex - 1);
                    String rhs = strLine.substring(arrowIndex + 3);
                    IntSet pointsTo = readPointsTo(rhs);
                    verifySrc(onlineGraph, lhs, pointsTo);
                }
            });
            System.err.println("OK");
        } finally {
            br.close();
        }
    }

    private static String getFileSuffix() {
        return "_" + booleanToBit(Configuration.USE_HVN) + booleanToBit(Configuration.USE_HRU) + booleanToBit(Configuration.USE_HCD);
    }

    private static String booleanToBit(boolean b) {
        return b ? "1" : "0";
    }

    /**
     * read the points-to from the file, and express it in terms of representatives
     */
    private static IntSet readPointsTo(String line) {
        IntSet result = new BddSet();
        // remove [ and ]
        line = line.substring(1, line.length() - 1);
        if (line.isEmpty()) {
            return result;
        }
        String[] ids = line.split(", ");
        for (String id1 : ids) {
            int id = Integer.parseInt(id1);
            result.add(id);
        }
        return result;
    }

    private static void verifySrc(final MultiGraph<OnlineNode> onlineGraph, String src, IntSet desired) {
        src = src.substring(1, src.length() - 1);
        if (src.isEmpty()) {
            throw new RuntimeException();
        }
        String[] ids = src.split(", ");
        TIntHashSet seen = new TIntHashSet();
        for (String id1 : ids) {
            int id = Integer.parseInt(id1);
            final OnlineNode node = onlineGraph.getNode(id, MethodFlag.NONE);
            OnlineNode nodeRep = node.getRep(onlineGraph, MethodFlag.NONE);
            if (!seen.add(nodeRep.id)) {
                continue;
            }
            // Something was left unprocessed. The condition is stronger than just checking the final points-to of the node:
            // the unprocessed state does not result on a wrong solution if the node has not outgoing copy/gep neighbors,
            // or the processing of diffPointsTo does not affect the state of the constraint graph
            if (!nodeRep.getPrevPointsTo().equals(nodeRep.pointsTo)) {
                final String errorMsg = "Error at node " + id +
                        ". The current points-to (1st line) is different from the previous points-to(2nd line):  " +
                        InputOutput.LINE_SEPARATOR + nodeRep.pointsTo + InputOutput.LINE_SEPARATOR + nodeRep.getPrevPointsTo();
                //System.err.println(errorMsg);
                //printNeighbors(onlineGraph, nodeRep);
                throw new IllegalStateException(errorMsg);
            }
            IntSet obtained = nodeRep.pointsTo;
            if (!obtained.equals(desired)) {
                final String errorMsg = "Error at node " + id +
                        ". The computed points-to (1st line) differs from the correct version (2nd line):  " +
                        InputOutput.LINE_SEPARATOR + obtained + InputOutput.LINE_SEPARATOR + desired;
                //System.err.println(errorMsg);
                //printNeighbors(onlineGraph, nodeRep);
                throw new IllegalStateException(errorMsg);
            }
        }
    }
}
