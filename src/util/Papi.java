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

File: Perfmon.java 

*/



package util;

import java.util.logging.Logger;

public class Papi {
  private static Logger logger = Logger.getLogger("util.Papi");
  private static boolean loaded = false;

  private static final String lib = "util_Papi";

  static {
    try {
      System.loadLibrary(lib);
      loaded = true;
    } catch (Error e) {
      logger.warning(String.format("Could not load library %s: %s", lib, e.toString()));
    }
  }

  private static native int _startSystem(String[] counters);
  private static native void _finishSystem();
  private static native void _readThread(int tid, long[] values);
  private static native void _startThread(int tid);
  private static native void _finishThread(int tid);
  
  public static int startSystem(String[] counters) {
    if (loaded)
      return _startSystem(counters);
    return 0;
  }
  
  public static void finishSystem() {
    if (loaded)
      _finishSystem();
  }
  
  public static void readThread(int tid, long[] values) {
    if (loaded)
      _readThread(tid, values);
  }
  
  public static void startThread(int tid) {
    if (loaded)
      _startThread(tid);
  }
  
  public static void finishThread(int tid) {
    if (loaded)
      _finishThread(tid);
  }
  
  public static boolean isLoaded() {
    return loaded;
  }
    
  private static class Test implements Runnable {
    private final int id;
    private final int iterations;
    private final long[] counters;
    public Test(int id, int events, int iterations) {
      this.id = id;
      this.iterations = iterations;
      counters = new long[events];
    }
    
    @Override
    public void run() {
      startThread(id);
      double c = 0.11;
      double a = 0.5;
      double b = 6.2;
      for (int i = 0; i < iterations; i++)
        c += a * b;
      readThread(id, counters);
      finishThread(id);
      double ipc = counters[1] != 0 ? (double) counters[0] / counters[1] : 0.0;
      System.out.printf("id: %d value: %f ipc: %f\n", id, c, ipc);
    }
  }
  
  public static void main(String[] args) throws Exception {
    int numThreads = 4;
    int iterations = 1000;
    
    switch (args.length) {
    default:
    case 2: iterations = Integer.parseInt(args[1]);
    case 1: numThreads = Integer.parseInt(args[0]);
    case 0:
    }
    
    int events = startSystem(new String[] { "PAPI_TOT_INS", "PAPI_TOT_CYC" });
    if (events < 2) {
      throw new Error("Could not initialize events");
    }
    try {
      Thread[] threads = new Thread[numThreads];
      for (int i = 0; i < numThreads; i++) {
        threads[i] = new Thread(new Test(i, events, iterations));
      }
      for (int i = 0; i < numThreads; i++) {
        threads[i].start();
      }
      for (int i = 0; i < numThreads; i++) {
        threads[i].join();
      }
    } finally {
      finishSystem();
    }
  }
}
