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

 File: BddNode.java
 */


package util.ints.bdd;

public final class BddNode {
  byte level;
  final BddNode low;
  final BddNode high;
  final int hash;
  BddNode next;

  public BddNode(byte level, BddNode low, BddNode high, int hash) {
    this(level, low, high, hash, null);
  }

  public BddNode(byte level, BddNode low, BddNode high, int hash, BddNode next) {
    this.level = level;
    this.low = low;
    this.high = high;
    this.hash = hash;
    this.next = next;
  }

  @Override
  public final int hashCode() {
    return hash;
  }

  public static int computeHash(byte level, BddNode low, BddNode high) {
    int result = level * 31;
    result = result * 31 ^ low.hash;
    return HashUtil.hash(result * 31 ^ high.hash);
  }

  @Override
  public boolean equals(Object obj) {
    return this == obj;
  }

  @Override
  public String toString() {
    if (this == Bdd.ZERO) {
      return "ZERO";
    }
    if (this == Bdd.ONE) {
      return "ONE";
    } else {
      return "(" + level + ", l = " + low + ", h = " + high + ")";
    }
  }
}
