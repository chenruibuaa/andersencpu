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

 File: BddDomain.java
 */


package util.ints.bdd;

import gnu.trove.list.array.TIntArrayList;
import util.ints.IntSetIterator;

import java.util.Arrays;

import static util.ints.bdd.Bdd.ONE;
import static util.ints.bdd.Bdd.ZERO;

public final class BddDomain {

    protected static BddDomain[] domain;
    protected static int fdvarnum;

    /* The index of this domain. */
    protected int index;
    /* Variable indices for the variable set */
    int[] ivar;
    /* Variable indices for the variable set */
    protected int[] absoluteIvar;
    /* The BDD variable set.  Constructed in extDomain() */
    protected BddNode var;
    /* correspondence between number and Bdd, for this domain */
    private final BddNode[] numAsBdd;

    private static IteratorCache<BddNode, TIntArrayList> iteratorCache;
    private static int initialNodeTableCapacity;
    private static int initialNodeTableSegments;

    public static void setup(int initNodeTableCapacity, int initNodeTableSegments, int initIteratorCacheCapacity) {
        initialNodeTableCapacity = initNodeTableCapacity;
        initialNodeTableSegments = initNodeTableSegments;
        iteratorCache = new IteratorCache<BddNode, TIntArrayList>(initIteratorCacheCapacity);
    }


    /**
     * Default constructor.
     *
     * @param index index of this domain
     * @param range size of this domain
     */
    protected BddDomain(int index, long range) {
        if (range <= 0) {
            throw new BddException();
        }
        this.index = index;
        long calcsize = 2L;
        int binsize = 1;
        while (calcsize < range) {
            binsize++;
            calcsize <<= 1;
        }
        ivar = new int[binsize];
        numAsBdd = new BddNode[(int) range];
    }

    public int binSize() {
        return ivar.length;
    }

    public BddNode getNodeVar(int n) {
        BddNode res = numAsBdd[n];
        if (res == null) {
            res = ithVar(n);
            numAsBdd[n] = res;
            return res;
        }
        return res;
    }

    /**
     * converts a number to a BDD
     *
     * @param val
     * @return the resulting BDD
     * @see "BDD buddy.fdd_ithvar(int var, int val)"
     */
    public BddNode ithVar(int val) {
        BddNode res = ONE;
        int binSize = ivar.length;
        for (int i = 0; i < binSize; i++) {
            if ((val & 1) == 1) {
                res = Bdd.and(res, Bdd.ithVar(ivar[i]));
            } else {
                res = Bdd.and(res, Bdd.nithVar(ivar[i]));
            }
            val >>= 1;
        }
        return res;
    }

    /**
     * Returns the variable set that contains the variables used to define this
     * finite domain block.
     *
     * @return BDD  the resulting BDD
     * @see "BDD buddy.fdd_ithset(int var)"
     */
    public BddNode set() {
        return var;
    }

    /**
     * Returns an integer array containing the indices of the BDD variables used
     * to define this finite domain.
     *
     * @return int[]  the resulting array
     * @see "int* buddy.fdd_vars(int var)"
     */
    public int[] vars() {
        return ivar;
    }

    public BddSetIterator iterator(final BddNode root) {
        return new BddSetIterator(getElements(root));
    }

    public TIntArrayList getElements(final BddNode root) {
        TIntArrayList elems = iteratorCache.get(root);
        if (elems == null) {
            BddAllSet bddAllSet = new BddAllSet(binSize());
            elems = bddAllSet.getAllSolutions(root);
            elems.sort();
            iteratorCache.put(root, elems);
        }
        return elems;
    }

    public int satCount(final BddNode node) {
        return (int) (Math.pow(2, index(node.level)) * satCount_rec(node));
    }

    private double satCount_rec(final BddNode node) {
        if (node == ONE) {
            return 1;
        }
        if (node == ZERO) {
            return 0;
        }
        // TODO: add cache, if the app frequently invokes the method
        BddNode low = node.low;
        BddNode high = node.high;
        int index = index(node.level) + 1;
        return satCount_rec(low) * Math.pow(2, index(low.level) - index) + satCount_rec(high)
                * Math.pow(2, index(high.level) - index);
    }

    // TODO: inneficient, can be replaced by array

    private int index(final int var) {
        int numVars = ivar.length;
        if (var == ONE.level || var == ZERO.level) {
            return numVars;
        }
        for (int i = 0; i < numVars; i++) {
            if (ivar[i] == var) {
                return i;
            }
        }
        throw new RuntimeException();
    }

    public static String toString(BddNode node) {
        StringBuilder sb = new StringBuilder();
        // SUPER KLUDGE
        BddDomain pointsToDomain = domain[0];
        BddNode ptsVar = pointsToDomain.set();
        BddDomain gepDomain = domain[1];
        BddNode gepVar = gepDomain.set();
        sb.append("(");
        sb.append(toString(pointsToDomain, Bdd.relProd(ONE, node, gepVar)));
        sb.append(", ");
        sb.append(toString(gepDomain, Bdd.relProd(ONE, node, ptsVar)));
        sb.append(")");
        return sb.toString();
    }

    public static String toString(BddDomain bddDomain, BddNode root) {
        StringBuilder stringBuilder = new StringBuilder("[");
        IntSetIterator iterator = bddDomain.iterator(root);
        while (iterator.hasNext()) {
            stringBuilder.append(iterator.nextInt()).append(", ");
        }
        int length = stringBuilder.length();
        if (length > 1) {
            // remove last comma and whitespace
            stringBuilder.deleteCharAt(length - 1);
            stringBuilder.deleteCharAt(length - 2);
        }
        stringBuilder.append("]");
        return stringBuilder.toString();
    }

    public static int numberOfDomains() {
        return fdvarnum;
    }

    public static BddDomain getDomain(int num) {
        return domain[num];
    }

    /**
     * Adds another set of finite domain blocks
     *
     * @param domainSizes
     * @return the resulting set of domains
     * @see "int buddy.fdd_extdomain(int *dom, int num)"
     */
    public static BddDomain[] extDomain(long[] domainSizes) {
        int extravars = 0;
        int bn;
        boolean more;

        int numDomains = domainSizes.length;
        domain = new BddDomain[numDomains];
        /* Create bdd variable tables */
        for (int n = 0; n < numDomains; n++) {
            BddDomain newDom = new BddDomain(n, domainSizes[n]);
            domain[n] = newDom;
            extravars += newDom.binSize();
        }
        Bdd bdd = new Bdd(extravars, initialNodeTableCapacity, initialNodeTableSegments);
        /* Set correct variable sequence (interleaved) */
        int binoffset = 0;
        for (bn = 0, more = true; more; bn++) {
            more = false;
            for (int n = 0; n < numDomains; n++) {
                BddDomain dom = domain[n];
                if (bn < dom.binSize()) {
                    more = true;
                    dom.ivar[bn] = binoffset;
                    binoffset++;
                }
            }
        }
        for (int n = 0; n < numDomains; n++) {
            BddDomain dom = domain[n];
            dom.var = Bdd.makeSet(dom.ivar);
        }
        fdvarnum = numDomains;
        return domain;
    }

    public static void reset() {
        if (domain != null) {
            for (BddDomain d : domain) {
                //d.numAsBdd = null;
                d.ivar = null;
            }
        }
        domain = null;
        fdvarnum = 0;

        Bdd.reset();
    }

    // A possible alternative would be to return a int[]
    // But it implies computing the number of solutions beforehand => slower in practice.

    private TIntArrayList getAllSolutions(BddNode root) {
        TIntArrayList ret = new TIntArrayList();
        while (root != ZERO) {
            int res = satOne(root);
            BddNode solution = getNodeVar(res);
            root = Bdd.diff(root, solution);
            ret.add(res);
        }
        return ret;
    }

    // ASSUMPTION: the Bdd is only defined for the current domain.

    private int satOne(BddNode root) {
        BddNode node = root;
        int solution = 0;
        int satBit = 0;
        int satLevel = ivar[0];
        while (node != ZERO && node != ONE) {
            while (node.level > satLevel) {
                satBit++;
                satLevel = ivar[satBit];
            }
            if (node.low == ZERO) {
                // include the current bit in the result
                solution |= (1 << satBit);
                node = node.high;
            } else {
                node = node.low;
            }
        }
        return solution;
    }

    private static class BddAllSet {
        final TIntArrayList solutions;
        final byte[] compressedSolution;

        BddAllSet(int binSize) {
            solutions = new TIntArrayList();
            compressedSolution = new byte[binSize];
            Arrays.fill(compressedSolution, (byte) -1);
        }

        private TIntArrayList getAllSolutions(BddNode root) {
            allSat(root);
            return solutions;
        }

        private void allSat(BddNode node) {
            if (node == Bdd.ONE) {
                decode(compressedSolution);
                return;
            }
            if (node == Bdd.ZERO) {
                return;
            }
            int varNumber = level2var(node.level);
            BddNode low = node.low;
            if (low != Bdd.ZERO) {
                compressedSolution[varNumber] = 0;
                //TODO: watch out: there is an assumption about the order here!
                int lowVarNumber = level2var(low.level);
                for (int i = varNumber + 1; i < lowVarNumber; i++) {
                    compressedSolution[i] = -1;
                }
                allSat(low);
            }
            BddNode high = node.high;
            if (high != Bdd.ZERO) {
                compressedSolution[varNumber] = 1;
                //TODO: watch out: there is an assumption about the order here!
                int highVarNumber = level2var(high.level);
                for (int i = varNumber + 1; i < highVarNumber; i++) {
                    compressedSolution[i] = -1;
                }
                allSat(high);
            }
        }

        private void decode(byte[] compressedSolution) {
            int dontCareSolution = 0;
            int binSize = compressedSolution.length;
            int[] dontCarePositions = new int[binSize];
            int numberOfDontCare = 0;
            for (int i = 0; i < binSize; i++) {
                int valPosition = compressedSolution[i];
                int twoToTheI = (1 << i);
                if (valPosition == -1) {
                    dontCarePositions[numberOfDontCare] = twoToTheI;
                    numberOfDontCare++;
                } else if (valPosition == 1) {
                    dontCareSolution |= twoToTheI;
                }
            }
            if (numberOfDontCare == 0) {
                solutions.add(dontCareSolution);
            } else if (numberOfDontCare == 1) {
                solutions.add(dontCareSolution);
                solutions.add(dontCareSolution | dontCarePositions[0]);
            } else if (numberOfDontCare == 2) {
                solutions.add(dontCareSolution);
                int lowerBitTrue = dontCareSolution | dontCarePositions[0];
                solutions.add(lowerBitTrue);
                int upperBitTrue = dontCareSolution | dontCarePositions[1];
                solutions.add(upperBitTrue);
                solutions.add(upperBitTrue | lowerBitTrue);
            } else {
                int numSols = 1 << numberOfDontCare;
                for (int i = 0; i < numSols; i++) {
                    int solution = dontCareSolution;
                    for (int j = 0; j < numberOfDontCare; j++) {
                        int m = 1 << j;
                        if ((i & m) != 0) {
                            solution |= dontCarePositions[j];
                        }
                    }
                    solutions.add(solution);
                }
            }
        }

        // TODO: kludge, assumes homogeneous interleaving of two domains
        int level2var(int var) {
            return var >> (domain.length - 1);
        }
    }

    public static class BddSetIterator implements IntSetIterator {
        private final TIntArrayList solution;
        private int index;
        private final int SOLUTION_SIZE;

        BddSetIterator(TIntArrayList solution) {
            this.solution = solution;
            index = 0;
            SOLUTION_SIZE = solution.size();
        }

        @Override
        public boolean hasNext() {
            return index < SOLUTION_SIZE;
        }


        @Override
        public Integer next() {
            return nextInt();
        }

        @Override
        public int nextInt() {
            return solution.getQuick(index++);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
