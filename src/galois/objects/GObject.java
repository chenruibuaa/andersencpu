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

File: GObject.java 

 */

package galois.objects;

import galois.runtime.Iteration;

public interface GObject {
  /**
   * Accesses this object. Appropriate place to add hooks into the
   * runtime system to implement conflict detection and rollback.
   * 
   * <p>
   * This method should be called before each access to a Galois object.
   * Many implementing classes leave this implementation empty and
   * instead implement conflict detection and rollback for each method
   * of the object (e.g., {@link GSet}, {@link galois.objects.graph.Graph}, etc),
   * which is also known as "boosting".</p>
   * 
   * @param flags Galois runtime actions (e.g., conflict detection) that need to be executed
   *              upon invocation of this method. See {@link galois.objects.MethodFlag}
   */
  public void access(Iteration it, byte flags);
}
