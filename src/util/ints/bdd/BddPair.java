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

 File: BddPair.java
 */


package util.ints.bdd;

public final class BddPair {

  int numVars, last;
  final BddNode[] result;

  public BddPair() {
    numVars = Bdd.getNumVars();
    result = new BddNode[numVars];
    reset();
  }

  public void set(BddDomain p1, BddDomain p2) {
    int[] ivar1 = p1.vars();
    int[] ivar2 = p2.vars();
    set(ivar1, ivar2);
  }

  /**
   * Like set(), but with a whole list of pairs.
   * <p/>
   * Compare to
   * bdd_setpairs.
   */
  private void set(final int[] oldvar, final int[] newvar) {
    if (oldvar.length != newvar.length) {
      throw new BddException();
    }
    for (int n = 0; n < oldvar.length; n++) {
      setpair(oldvar[n], newvar[n]);
    }
  }

  private void setpair(int oldvar, int newvar) {
    int oldLevel = Bdd.var2Level(oldvar);
    result[oldLevel] = Bdd.ithVar(Bdd.level2Var(newvar));
    last = Math.max(oldLevel, last);
  }

  public void reset() {
    for (int i = 0; i < numVars; i++) {
      result[i] = Bdd.ithVar(Bdd.level2Var(i));
    }
    last = 0;
  }
}
