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

 File: Operation.java
 */


package util.ints.bdd;

public class Operation {
  // same codes as Buddy
  public static final byte AND = 0;
  public static final byte XOR = 1;
  public static final byte OR = 2;
  public static final byte NAND = 3;
  public static final byte NOR = 4;
  public static final byte IMP = 5;
  public static final byte BIIMP = 6;
  public static final byte DIFF = 7;
  public static final byte LESS = 8;
  public static final byte INV_VIMP = 9;
  //unary operators
  public static final byte NOT = 10;
  public static final byte QUANT = 11;
  public static final byte REPLACE = 12;
  // binary operators, other
  public static final byte RELPROD = 13;

  final byte operation;
  final BddNode op1, op2;

  public Operation(byte operation, BddNode op1, BddNode op2) {
    this.operation = operation;
    this.op1 = op1;
    this.op2 = op2;
  }

  @Override
  public boolean equals(Object obj) {
    return this.equals(obj);
  }

  public static boolean equals(byte operation, BddNode op1, BddNode op2, Object obj) {
    if (obj == null) {
      return false;
    }
    Operation op = (Operation) obj;
    if (isUnary(operation)) {
      return operation == op.operation && op1 == op.op1;
    }
    return operation == op.operation && op1 == op.op1 && op2 == op.op2;
  }

  @Override
  public int hashCode() {
    return computeHash(operation, op1, op2);
  }

  public static int computeHash(byte operation, BddNode op1, BddNode op2) {
    int result = operation * 31;
    result = (result * 31) ^ op1.hash;
    result = isUnary(operation) ? result : (result * 31) ^ op2.hash;
    return HashUtil.hash(result);
  }

  private static boolean isUnary(int op) {
    return op >= NOT && op <= REPLACE;
  }
}
