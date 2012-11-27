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


 */

package galois.runtime;

import galois.objects.GObject;
import galois.objects.MethodFlag;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import util.fn.Lambda0Void;

/**
 * Represents data accessed during an iteration. 
 */
public class Iteration {
  private static ThreadLocal<Iteration> iteration = new ThreadLocal<Iteration>();
  /**
   * A list of any commit actions
   */
  private final Deque<Lambda0Void> commitActions;
  private final List<ReleaseCallback> releaseActions;

  /**
   * A stack of callbacks to undo any actions the iteration has done
   */
  private final Deque<Lambda0Void> undoActions;

  /**
   * id of the iteration assigned on creation
   */
  private final int id;

  public Iteration(int id) {
    this.id = id;
    this.undoActions = new ArrayDeque<Lambda0Void>();
    this.releaseActions = new ArrayList<ReleaseCallback>();
    this.commitActions = new ArrayDeque<Lambda0Void>();
  }

  protected void reset() {
  }

  /**
   * Returns the currently executing iteration or null if no iteration is currently being
   * executed.
   *
   * @return  the current iteration
   */
  public static Iteration getCurrentIteration() {
    return iteration.get();
  }

  public static Iteration access(GObject obj, byte flags) {
    Iteration it = null;
    if (GaloisRuntime.needMethodFlag(flags, (byte) (MethodFlag.CHECK_CONFLICT | MethodFlag.SAVE_UNDO))) {
      it = Iteration.getCurrentIteration();
      if (it != null)
        obj.access(it, flags);
    }
    return it;
  }
  
  public static Iteration access(Object obj, byte flags) {
    Iteration it = null;
    if (GaloisRuntime.needMethodFlag(flags, (byte) (MethodFlag.CHECK_CONFLICT | MethodFlag.SAVE_UNDO))) {
      throw new Error();
    }
    return it;
  }
  
  public static Iteration access(Iteration it, GObject obj, byte flags) {
    if (it != null && GaloisRuntime.needMethodFlag(flags, (byte) (MethodFlag.CHECK_CONFLICT | MethodFlag.SAVE_UNDO))) {
      obj.access(it, flags);
    }
    return it;
  }
  
  public static Iteration access(Iteration it, Object obj, byte flags) {
    if (it != null && GaloisRuntime.needMethodFlag(flags, (byte) (MethodFlag.CHECK_CONFLICT | MethodFlag.SAVE_UNDO))) {
      throw new Error();
    }
    return it;
  }
  
  /**
   * Hack to set the current iteration being executed by a thread.
   *
   * @param it  the current iteration
   */
  static void setCurrentIteration(Iteration it) {
    iteration.set(it);
  }

  void addCommitAction(Lambda0Void c) {
    commitActions.addLast(c);
  }

  void addUndoAction(Lambda0Void c) {
    undoActions.addFirst(c);
  }

  /**
   * Add a new conflict log to the iteration (i.e. the iteration has made
   * calls listed in this CL)
   *
   */
  void addReleaseAction(ReleaseCallback callback) {
    releaseActions.add(callback);
  }

  /**
   * Clears undo logs, commit logs, conflict logs
   */
  protected int clearLogs(boolean releaseLocks) {
    undoActions.clear();
    commitActions.clear();

    if (releaseLocks)
      return releaseLocks();
    else
      return 0;
  }

  private int releaseLocks() {
    int total = 0;
    for (int i = 0; i < releaseActions.size(); i++) {
      total += releaseActions.get(i).release(this);
    }
    releaseActions.clear();

    return total;
  }

  /**
   * Called to abort an iteration. This unwinds the undo log, clears conflict
   * logs and releases all held partitions
   */
  int performAbort() {
    Lambda0Void c;
    while ((c = undoActions.poll()) != null) {
      c.call();
    }

    return clearLogs(true);
  }

  /**
   * Commit iteration. This clears the conflict logs and releases any held
   * partitions, and performs any commit actions
   */
  int performCommit(boolean releaseLocks) {
    Lambda0Void c;
    while ((c = commitActions.poll()) != null) {
      c.call();
    }

    return clearLogs(releaseLocks);
  }

  public int getId() {
    return id;
  }
}
