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

 File: IntPair.java
 */


package util.ints;

public class IntPair implements Comparable<IntPair> {
  public int first;
  public int second;

  public IntPair(int first, int second) {
    this.first = first;
    this.second = second;
  }

  public int getFirst() {
    return first;
  }

  public int getSecond() {
    return second;
  }

  public void setFirst(int i) {
    first = i;
  }

  public void setSecond(int i) {
    second = i;
  }

  @Override
  public String toString() {
    return "(" + first + ", " + second + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof IntPair)) {
      return false;
    }
    IntPair intPair = (IntPair) o;
    return first == intPair.first && second == intPair.second;
  }

  @Override
  public int hashCode() {
    int result = 31 * first + 17;
    return result * second;
  }

  @Override
  public int compareTo(IntPair intPair) {
    int ret = first - intPair.first;
    return ret != 0 ? ret : second - intPair.second;
  }
}
