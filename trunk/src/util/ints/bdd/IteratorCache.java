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

 File: IteratorCache.java
 */
package util.ints.bdd;

import util.Pair;

import java.lang.reflect.Array;

final class IteratorCache<K, V> {
    private static final int DEFAULT_BUCKETS = 2 << 12;
    private final int MASK;
    private final Pair<K, V>[] buckets;


    public IteratorCache() {
        this(DEFAULT_BUCKETS);
    }

    @SuppressWarnings("unchecked")
    public IteratorCache(int numBuckets) {
        buckets = (Pair[]) Array.newInstance(Pair.class, numBuckets);
        // assumes that numBuckets is a power of 2
        MASK = numBuckets - 1;
    }

    public void put(K key, V value) {
        int index = getIndex(key);
        buckets[index] = new Pair<K, V>(key, value);
    }

    public V get(K key) {
        int index = getIndex(key);
        Pair<K, V> element = buckets[index];
        if (element != null && element.getFirst() == key) {
            return element.getSecond();
        }
        return null;
    }

    private int getIndex(K key) {
        int hash = key.hashCode();
        return hash & MASK;
    }
}
