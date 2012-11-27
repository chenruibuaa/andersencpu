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

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.logging.Logger;

public class Vtune {
  private static boolean loaded;
  private static Logger logger = Logger.getLogger("util.Vtune");
  private static final String lib = "util_Vtune";

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

  private static native void _resume();

  private static native void _pause();

  private static void sendPacket(int port) {
    try {
      Socket s = new Socket(InetAddress.getLocalHost(), port);
      byte[] data = "hello\n".getBytes("US-ASCII");
      s.getOutputStream().write(data);
      s.close();
      return;
    } catch (UnknownHostException e) {
      logger.warning("Could not connect to socket: " + e);
    } catch (IOException e) {
      logger.warning("Could not connect to socket: " + e);
    }
  }

  public static void resume() {
    if (loaded) {
      _resume();
      sendPacket(8888);
    }
  }

  public static void pause() {
    if (loaded) {
      _pause();
      sendPacket(8889);
    }
  }
}
