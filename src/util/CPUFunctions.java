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

File: CPUFunctions.java 

*/



package util;

import java.util.logging.Logger;

/**
 * JNI library to certain CPU instructions and functions
 * 
 *
 */
public class CPUFunctions {
  private static boolean loaded;
  private static Logger logger = Logger.getLogger("util.CPUFunctions");
  private static final String lib = "util_CPUFunctions";
  
  static {
    try {
      System.loadLibrary(lib);
      loaded = true;
    } catch (Error e) {
      logger.warning(String.format("Could not load library %s: %s", lib, e.toString()));
    }
  }

  /**
   * @return true if the library been successfully loaded
   */
  public static boolean isLoaded() {
    return loaded;
  }

  private static native int _getCpuId();
  private static native void _setThreadId(int id);
  private static native int _getThreadId();
  private static native void _setThreadAffinity(int id);
  
  public static int getThreadId() {
    if (loaded)
      return _getThreadId();
    else
      return -1;
  }
  
  public static void setThreadId(int id) {
    if (loaded)
      _setThreadId(id);
  }
  
  public static void setThreadAffinity(int id) {
    if (loaded)
      _setThreadAffinity(id);
  }
  
  /**
   * @return  the apicid of the currently executing thread or -1 if the JNI library was
   *   not loaded
   */
  public static int getCpuId() {
    if (loaded)
      return _getCpuId();
    else
      return -1;
  }

  public static void main(String[] args) {
    System.out.println(getCpuId());
  }
}
