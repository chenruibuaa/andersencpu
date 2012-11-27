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

 File: BitVector.java
 */


package util.ints.bdd;

import java.util.Arrays;

import static util.ints.bdd.Bdd.ONE;
import static util.ints.bdd.Bdd.ZERO;

public class BitVector {
  private final BddNode[] bitvec;

  public BitVector(int size) {
    bitvec = new BddNode[size];
    Arrays.fill(bitvec, Bdd.ZERO);
  }

  /**
   * Builds a boolean vector representing an integer value.
   *
   * @param bitnum
   * @param num
   * @return the resulting bit vector
   * @see "bvec buddy.bvec_con(int bitnum, int val)"
   */
  public static BitVector con(int bitnum, int num) {
    BitVector bitVector = new BitVector(bitnum);
    BddNode[] bitvec = bitVector.bitvec;
    final int length = bitvec.length;
    for (int i = 0; i < length; i++) {
      bitvec[i] = ((num & 1) == 1) ? ONE : ZERO;
      num >>= 1;
    }
    return bitVector;
  }

  /**
   * Builds a boolean vector for addition
   *
   * @param bitVector1
   * @param bitVector2
   * @return the resulting bit vector
   * @see "bvec buddy.bvec_add(bvec l, bvec r)"
   */
  public static BitVector add(final BitVector bitVector1, final BitVector bitVector2) {
    BddNode[] bitvec1 = bitVector1.bitvec;
    BddNode[] bitvec2 = bitVector2.bitvec;
    int length = bitvec1.length;
    if (length != bitvec2.length) {
      throw new BddException();
    }
    BitVector res = new BitVector(length);
    BddNode carry = ZERO;
    BddNode bddNode1, bddNode2;
    for (int i = 0; i < length; i++) {
      /* bitvec[n] = l[n] ^ r[n] ^ c; */
      bddNode1 = bitvec1[i];
      bddNode2 = bitvec2[i];
      BddNode tmp = Bdd.xor(bddNode1, bddNode2);
      res.bitvec[i] = Bdd.xor(tmp, carry);
      /* c = (l[n] & r[n]) | (c & (l[n] | r[n])); */
      tmp = Bdd.and(carry, Bdd.or(bddNode1, bddNode2));
      carry = Bdd.or(tmp, Bdd.and(bddNode1, bddNode2));
    }
    return res;
  }

  /**
   * Calculates the truth value of x = y
   *
   * @param bitVector1
   * @param bitVector2
   * @return the resulting BDD
   * @see "bdd buddy.bvec_equ(bvec l, bvec r)"
   */
  public static BddNode equ(final BitVector bitVector1, final BitVector bitVector2) {
    BddNode[] bitvec1 = bitVector1.bitvec;
    BddNode[] bitvec2 = bitVector2.bitvec;
    int length = bitvec1.length;
    if (length != bitvec2.length) {
      throw new BddException();
    }
    BddNode res = ONE;
    BddNode bddNode1, bddNode2;
    for (int i = 0; i < length; i++) {
      bddNode1 = bitvec1[i];
      bddNode2 = bitvec2[i];
      res = Bdd.and(res, Bdd.biimp(bddNode1, bddNode2));
    }
    return res;
  }

  /**
   * builds a boolean vector from a FDD variable block
   *
   * @see "bvec buddy.bvec_varfdd(int var)"
   */
  public static BitVector varfdd(final BddDomain bddDomain) {
    int numBits = bddDomain.binSize();
    BitVector res = new BitVector(numBits);
    for (int i = 0; i < numBits; i++) {
      res.bitvec[i] = Bdd.ithVar(bddDomain.ivar[i]);
    }
    return res;
  }

  public int bitNum() {
    return bitvec.length;
  }
}
