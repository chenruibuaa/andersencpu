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

File: AbstractBaseObject.java 

 */

package galois.objects;

import galois.runtime.GaloisRuntime;
import galois.runtime.Iteration;
import galois.runtime.ReleaseCallback;
import util.Intrinsics;

/**
 * Default implementation of a Galois object suitable for extension
 * by user code. It encodes the following policies:
 *  <ul>
 *   <li><i>exclusive access:</i> only one iteration can access an object
 *       at a time and</li>
 *   <li><i>user-defined restore:</i> rollback is implemented by user,
 *      probably by inverse actions.</li>
 *  </ul>
 * <p>
 * If there aren't reasonable inverse actions for this object, consider
 * using {@link AbstractBaseObject} instead.
 */
public abstract class AbstractNoUndoBaseObject implements GObject, ReleaseCallback {
  private static final long ownerOffset = Intrinsics.objectFieldOffset("owner", AbstractNoUndoBaseObject.class);
  private Object owner;

  private void acquire(Iteration it) {
    if (owner == it)
      return;

    while (!Intrinsics.compareAndSetObject(this, ownerOffset, null, it)) {
      GaloisRuntime.getRuntime().raiseConflict(it, (Iteration) owner);
    }

    GaloisRuntime.getRuntime().onRelease(it, this);
  }

  @Override
  public int release(Iteration it) {
    owner = null;
    return 1;
  }

  @Override
  public final void access(Iteration it, byte flags) {
    if (GaloisRuntime.needMethodFlag(flags, MethodFlag.CHECK_CONFLICT)) {
      acquire(it);
    }
  }
  
  protected final boolean tryAccess(Iteration it, byte flags) {
    if (GaloisRuntime.needMethodFlag(flags, MethodFlag.CHECK_CONFLICT)) {
      if (owner == it)
        return true;

      if (Intrinsics.compareAndSetObject(this, ownerOffset, null, it)) {
        GaloisRuntime.getRuntime().onRelease(it, this);
        return true;
      }

      return false;
    }
    return true;
  }
}
