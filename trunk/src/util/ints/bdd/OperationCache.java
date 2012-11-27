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

 File: OperationCache.java
 */
package util.ints.bdd;

import java.lang.reflect.Array;
import java.util.Arrays;


public final class OperationCache {
    private static final int DEFAULT_BUCKETS = 2 << 12;
    private final int MASK;
    private final Element[] buckets;

    public OperationCache() {
        this(DEFAULT_BUCKETS);
    }

    public OperationCache(int numBuckets) {
        // numBuckets has to be a power of 2
        buckets = (Element[]) Array.newInstance(Element.class, numBuckets);
        MASK = numBuckets - 1;
    }

    public void put(byte operation, BddNode op1, BddNode op2, BddNode value) {
        int index = getIndex(operation, op1, op2);
        buckets[index] = new Element(operation, op1, op2, value);
    }

    public BddNode get(byte operation, BddNode op1, BddNode op2) {
        int index = getIndex(operation, op1, op2);
        Element element = buckets[index];
        if (Operation.equals(operation, op1, op2, element)) {
            return element.value;
        }
        return null;
    }

    private int getIndex(byte operation, BddNode op1, BddNode op2) {
        int hash = Operation.computeHash(operation, op1, op2);
        return hash & MASK;
    }

    public void clear() {
        // assist GC
        Arrays.fill(buckets, null);
    }

    private final class Element extends Operation {
        private final BddNode value;

        Element(byte operation, BddNode op1, BddNode op2, BddNode value) {
            super(operation, op1, op2);
            this.value = value;
        }
    }

}
