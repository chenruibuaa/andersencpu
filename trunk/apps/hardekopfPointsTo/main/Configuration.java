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

 File: Configuration.java
 */

package hardekopfPointsTo.main;

import galois.runtime.GaloisRuntime;
import util.SystemProperties;

public final class Configuration {

    static final boolean PRINT_CONSTRAINTS;

    static final boolean PRINT_HCD_TABLE;

    // print the results of the analysis to a file
    static final boolean PRINT_SOLUTION;

    // check that the result of the analysis matches Ben's solution
    // 0 = never; 1 = only the first run; 2 = always
    static int VERIFY_RESULT;

    // also verify the number of nodes/constraints/etc after the offline phase
    // The goal is to identify the source of a bug before it propagates to the online analysis
    // 0 = never; 1 = only the first run; 2 = always
    static int VERIFY_OFFLINE;

    static final boolean USE_HVN;

    static final boolean USE_HRU;

    // use hybrid cycle detection ?
    static final boolean USE_HCD;

    static final boolean USE_BDD_ADDER;

    // use Ben's analysis (true) or Wave Analysis (false)
    static final boolean BEN_ANALYSIS = SystemProperties.getBooleanProperty("galois.hardekopfPointsTo.ben", true);

    static final int NUM_VIRTUAL_THREADS;

    static final int CHUNK_SIZE = 32;

    static final float CAPACITY = 0.75f;

    static final int CONCURRENCY_LEVEL = 1 << 9;
    public static final boolean PRINT_SOLUTION_GRAPH;

    static {
        PRINT_CONSTRAINTS = false;
        PRINT_HCD_TABLE = false;
        PRINT_SOLUTION = false;
        PRINT_SOLUTION_GRAPH = false;
        VERIFY_RESULT = SystemProperties.getIntProperty("galois.hardekopfPointsTo.verify", 2);
        USE_BDD_ADDER = SystemProperties.getBooleanProperty("galois.hardekopfPointsTo.adder", false);
        USE_HVN = SystemProperties.getBooleanProperty("galois.hardekopfPointsTo.hvn", false);
        USE_HRU = SystemProperties.getBooleanProperty("galois.hardekopfPointsTo.hru", false);
        USE_HCD = SystemProperties.getBooleanProperty("galois.hardekopfPointsTo.hcd", true);
        // disable unless HVN=HRU=HCD=true
        VERIFY_OFFLINE = (USE_HVN && USE_HRU && USE_HCD) ? VERIFY_RESULT : 0;
        //  a relatively high decomposition factor works better in practice
        NUM_VIRTUAL_THREADS = getNumThreads() * SystemProperties.getIntProperty("galois.hardekopfPointsTo.od", 8);
    }

    public static String printConfiguration() {
        String res = "{";
        res += "hvn= " + USE_HVN;
        res += ", hru= " + USE_HRU;
        res += ", hcd= " + USE_HCD;
        res += ", bddAdd= " + USE_BDD_ADDER;
        res += ", alg= " + (BEN_ANALYSIS ? "hardekopf" : "wave prop.");
        res += "}";
        return res;
    }

    static int getChunkSize(final int size) {
        return Math.max(size / NUM_VIRTUAL_THREADS, 1);
    }

    static int getSequentialChunkSize(final int size) {
        return size;
    }

    public static int getNumThreads() {
        return GaloisRuntime.getRuntime().getMaxThreads();
    }

    public static int getChunk() {
        return CHUNK_SIZE;
    }
}
