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

 File: Bdd.java
 */


package util.ints.bdd;


public class Bdd {

    // reserve one entry in the cache table for every #number of entries in the initial node table
    // has to be a power of 2
    private static final int NODE_TO_CACHE_ENTRY_RATIO = 128;

    private static BddNodeTable nodeTable;
    private static OperationCache opCache;

    private static int numberOfVariables;
    private static BddNode[] varList, nvarList;
    public static BddNode[] numbers;

    public static final BddNode ZERO = new BddNode((byte) -1, null, null, 0);
    public static final BddNode ONE = new BddNode((byte) -1, null, null, 1);

    /* Operator results - entry = left<<1 | right  (left,right in {0,1}) */
    public static final BddNode[][] OPR_RES = {{ZERO, ZERO, ZERO, ONE}, // and                       ( & )
            {ZERO, ONE, ONE, ZERO}, // xor                       ( ^ )
            {ZERO, ONE, ONE, ONE}, // or                        ( | )
            {ONE, ONE, ONE, ZERO}, // nand
            {ONE, ZERO, ZERO, ZERO}, // nor
            {ONE, ONE, ZERO, ONE}, // implication               ( >> )
            {ONE, ZERO, ZERO, ONE}, // bi-implication
            {ZERO, ZERO, ONE, ZERO}, // difference /greater than  ( - ) ( > )
            {ZERO, ONE, ZERO, ZERO}, // less than                 ( < )
            {ONE, ZERO, ONE, ONE}, // inverse implication       ( << )
            {ONE, ONE, ZERO, ZERO}}; // not                       ( ! )

    public Bdd(int numVars, int initialCapacity, int initialNumSegments) {
        numberOfVariables = numVars;
        ZERO.level = (byte) numVars;
        ONE.level = (byte) numVars;
        varList = new BddNode[numberOfVariables];
        nvarList = new BddNode[numberOfVariables];
        nodeTable = new BddNodeTable(initialCapacity, 0.75f, initialNumSegments);
        nodeTable.putIfAbsent(ZERO);
        nodeTable.putIfAbsent(ONE);
        int opCacheInitialCapacity = initialCapacity / NODE_TO_CACHE_ENTRY_RATIO;
        opCache = new OperationCache(opCacheInitialCapacity);
        for (byte i = 0; i < numberOfVariables; i++) {
            createVar(i);
        }
    }

    public static void reset() {
        if (nodeTable != null) {
            nodeTable.clear();
        }
        if (opCache != null) {
            opCache.clear();
        }
        varList = null;
        nvarList = null;
    }

    /**
     * create a new BDD variable
     */
    public static void createVar(byte v) {
        varList[v] = nodeTable.putBddNodeIfAbsentReturnKey(v, ZERO, ONE);
        nvarList[v] = nodeTable.putBddNodeIfAbsentReturnKey(v, ONE, ZERO);
    }

    /**
     * Creates a new node and inserts it into the BDD table, or if the node
     * is already present, returns the existing node
     *
     * @return an existing node if already present in the BDD or else a new node
     */
    private static BddNode mk(byte v, final BddNode l, final BddNode h) {
        if (l == h) {
            return l;
        }
        return nodeTable.putBddNodeIfAbsentReturnKey(v, l, h);
    }

    public static boolean isEmpty(final BddNode bddNode) {
        return bddNode == ZERO;
    }

    /**
     * @param l the left argument
     * @param r the right argument
     * @return l AND r
     * @see #or
     */
    public static BddNode and(BddNode l, BddNode r) {
        if (l == r || r == ONE) {
            return l;
        }
        if (l == ZERO || r == ZERO) {
            return ZERO;
        }
        if (l == ONE) {
            return r;
        }
        byte v = l.level;
        if (l.level > r.level) {
            BddNode temp = l;
            l = r;
            r = temp;
            v = l.level;
        }
        BddNode x, y;
        BddNode cached = opCache.get(Operation.AND, l, r);
        if (cached != null) {
            return cached;
        }
        if (v == r.level) {
            x = and(l.low, r.low);
            y = and(l.high, r.high);
        } else {
            x = and(l.low, r);
            y = and(l.high, r);
        }
        BddNode ret = mk(v, x, y);
        opCache.put(Operation.AND, l, r, ret);
        return ret;
    }

    /**
     * @param l the left argument
     * @param r the right argument
     * @return l OR r
     * @see #and
     */
    public static BddNode or(BddNode l, BddNode r) {
        if (l == ONE || r == ONE) {
            return ONE;
        }
        if (l == ZERO || l == r) {
            return r;
        }
        if (r == ZERO) {
            return l;
        }
        byte v = l.level;
        if (l.level > r.level) {
            BddNode temp = l;
            l = r;
            r = temp;
            v = l.level;
        }
        BddNode cached = opCache.get(Operation.OR, l, r);
        if (cached != null) {
            return cached;
        }
        BddNode x, y;
        if (v == r.level) {
            x = or(l.low, r.low);
            y = or(l.high, r.high);
        } else {
            x = or(l.low, r);
            y = or(l.high, r);
        }
        BddNode ret = mk(v, x, y);
        opCache.put(Operation.OR, l, r, ret);
        return ret;
    }

    /**
     * @param u1 the left argument
     * @param u2 the right argument
     * @return u1 XOR u2
     */
    public static BddNode xor(BddNode u1, BddNode u2) {
        if (u1 == u2) {
            return ZERO;
        }
        if (u1 == ZERO) {
            return u2;
        }
        if (u2 == ZERO) {
            return u1;
        }
        if (u1 == ONE) {
            return not(u2);
        }
        if (u2 == ONE) {
            return not(u1);
        }
        byte v = u1.level;
        if (u1.level > u2.level) {
            BddNode temp = u1;
            u1 = u2;
            u2 = temp;
            v = u1.level;
        }
        BddNode cached = opCache.get(Operation.XOR, u1, u2);
        if (cached != null) {
            return cached;
        }
        BddNode l, h;
        if (v == u2.level) {
            l = xor(u1.low, u2.low);
            h = xor(u1.high, u2.high);
        } else {
            l = xor(u1.low, u2);
            h = xor(u1.high, u2);
        }
        BddNode ret = mk(v, l, h);
        opCache.put(Operation.XOR, u1, u2, ret);
        return ret;
    }

    public static BddNode biimp(BddNode u1, BddNode u2) {
        if (u1 == u2) {
            return ONE;
        }
        if (u1 == ZERO) {
            return not(u2);
        }
        if (u2 == ZERO) {
            return not(u1);
        }
        if (u1 == ONE) {
            return u2;
        }
        if (u2 == ONE) {
            return u1;
        }
        byte v = u1.level;
        if (u1.level > u2.level) {
            BddNode temp = u1;
            u1 = u2;
            u2 = temp;
            v = u1.level;
        }
        BddNode cached = opCache.get(Operation.BIIMP, u1, u2);
        if (cached != null) {
            return cached;
        }
        BddNode l, h;
        if (v == u2.level) {
            l = biimp(u1.low, u2.low);
            h = biimp(u1.high, u2.high);
        } else {
            l = biimp(u1.low, u2);
            h = biimp(u1.high, u2);
        }
        BddNode ret = mk(v, l, h);
        opCache.put(Operation.BIIMP, u1, u2, ret);
        return ret;
    }

    /**
     * Computes the set difference
     *
     * @param l the left argument
     * @param r the right argument
     * @return l - r
     */
    public static BddNode diff(final BddNode l, final BddNode r) {
        if (r == ONE || l == ZERO || l == r) {
            return ZERO;
        }
        if (r == ZERO) {
            return l;
        }
        if (l == ONE) {
            return not(r);
        }
        BddNode x, y;
        BddNode cached = opCache.get(Operation.DIFF, l, r);
        if (cached != null) {
            return cached;
        }
        byte v;
        if (l.level == r.level) {
            v = l.level;
            x = diff(l.low, r.low);
            y = diff(l.high, r.high);
        } else if (l.level < r.level) {
            v = l.level;
            x = diff(l.low, r);
            y = diff(l.high, r);
        } else {
            v = r.level;
            x = diff(l, r.low);
            y = diff(l, r.high);
        }
        BddNode ret = mk(v, x, y);
        opCache.put(Operation.DIFF, l, r, ret);
        return ret;
    }

    public static BddNode nand(BddNode l, BddNode r) {
        // TODO: implement native version if it is used frequently by your app
        return apply(l, r, Operation.NAND);
    }

    public static BddNode nor(BddNode l, BddNode r) {
        // TODO: implement native version if it is used frequently by your app
        return apply(l, r, Operation.NOR);
    }

    public static BddNode imp(BddNode l, BddNode r) {
        // TODO: implement native version if it is used frequently by your app
        return apply(l, r, Operation.IMP);
    }

    public static BddNode less(BddNode l, BddNode r) {
        // TODO: implement native version if it is used frequently by your app
        return apply(l, r, Operation.LESS);
    }

    public static BddNode invimp(BddNode l, BddNode r) {
        // TODO: implement native version if it is used frequently by your app
        return apply(l, r, Operation.INV_VIMP);
    }

    /**
     * @param u argument
     * @return NOT u
     */
    public static BddNode not(BddNode u) {
        if (u == ONE) {
            return ZERO;
        }
        if (u == ZERO) {
            return ONE;
        }
        BddNode cached = opCache.get(Operation.NOT, u, null);
        if (cached != null) {
            return cached;
        }
        BddNode l = not(u.low);
        BddNode h = not(u.high);
        BddNode ret = mk(u.level, l, h);
        opCache.put(Operation.NOT, u, null, ret);
        return ret;
    }

    /**
     * Returns the number of elements in the BDD, starting at a certain node.
     *
     * @param node root of the subtree whose size will be computed
     * @return number of elements in the set represented by the BDD rooted at the given node
     */
    public static int satCount(BddNode node) {
        return (int) (Math.pow(2, node.level) * satCount_rec(node));
    }

    protected static double satCount_rec(BddNode node) {
        if (node == ONE) {
            return 1;
        }
        if (node == ZERO) {
            return 0;
        }
        // TODO: add cache
        return satCount_rec(node.low) * Math.pow(2, node.low.level - node.level - 1) + satCount_rec(node.high)
                * Math.pow(2, node.high.level - node.level - 1);
    }

    /**
     * Replaces the nodes in a BDD by their counterparts (as defined by pair)
     *
     * @param node
     * @param pair
     * @return the result of applying the replacement
     * @see "BDD buddy.bdd_replace(BDD r, bddPair *pair)"
     */
    public static BddNode replace(BddNode node, final BddPair pair) {
        int last = pair.last;
        if (node == ZERO || node == ONE || node.level > last) {
            return node;
        }
        // TODO: given that for now there is only one BddPair, we can simply
        // store a unary operation
        BddNode cached = opCache.get(Operation.REPLACE, node, null);
        if (cached != null) {
            return cached;
        }
        BddNode[] perm_vec = pair.result;
        BddNode l = replace(node.low, pair);
        BddNode r = replace(node.high, pair);
        BddNode res = bddCorrectify(perm_vec[node.level].level, l, r);
        opCache.put(Operation.REPLACE, node, null, res);
        return res;
    }

    private static BddNode bddCorrectify(int level, BddNode l, BddNode r) {
        if (level < l.level && level < r.level) {
            return mk((byte) level, l, r);
        }
        if (level == l.level || level == r.level) {
            throw new BddException();
        }
        if (l.level == r.level) {
            BddNode x = bddCorrectify(level, l.low, r.low);
            BddNode y = bddCorrectify(level, l.high, r.high);
            return mk(l.level, x, y);
        } else if (l.level < r.level) {
            BddNode x = bddCorrectify(level, l.low, r);
            BddNode y = bddCorrectify(level, l.high, r);
            return mk(l.level, x, y);
        } else {
            BddNode x = bddCorrectify(level, l, r.low);
            BddNode y = bddCorrectify(level, l, r.high);
            return mk(r.level, x, y);
        }
    }

    /**
     * Applies a basic BDD operation
     *
     * @param l   the left argument
     * @param r   the right argument
     * @param opr the operation
     * @return the result of applying the operation
     * @see "BDD buddy.bdd_apply(BDD left, BDD right, int opr)"
     */
    private static BddNode apply(BddNode l, BddNode r, byte opr) {
        // TODO: verify that the operator is correct
        switch (opr) {
            case Operation.AND:
                if (l == r) {
                    return l;
                }
                if (l == ZERO || r == ZERO) {
                    return ZERO;
                }
                if (l == ONE) {
                    return r;
                }
                if (r == ONE) {
                    return l;
                }
                break;
            case Operation.OR:
                if (l == r) {
                    return l;
                }
                if (l == ONE || r == ONE) {
                    return ONE;
                }
                if (l == ZERO) {
                    return r;
                }
                if (r == ZERO) {
                    return l;
                }
                break;
            case Operation.XOR:
                if (l == r) {
                    return ZERO;
                }
                if (l == ZERO) {
                    return r;
                }
                if (r == ZERO) {
                    return l;
                }
                break;
            case Operation.NAND:
                if (l == ZERO || r == ZERO) {
                    return ONE;
                }
                break;
            case Operation.NOR:
                if (l == ONE || r == ONE) {
                    return ZERO;
                }
                break;
            case Operation.IMP:
                if (l == ZERO) {
                    return ONE;
                }
                if (l == ONE) {
                    return r;
                }
                if (r == ONE) {
                    return ONE;
                }
                break;
        }
        int lNumber = terminalToNumber(l);
        int rNumber = terminalToNumber(r);
        if (isTerminal(lNumber) && isTerminal(rNumber)) {
            return OPR_RES[opr][lNumber << 1 | rNumber];
        }
        BddNode cached = opCache.get(opr, l, r);
        if (cached != null) {
            return cached;
        }
        BddNode res;
        if (l.level == r.level) {
            BddNode x = apply(l.low, r.low, opr);
            BddNode y = apply(l.high, r.high, opr);
            res = mk(l.level, x, y);
        } else if (l.level < r.level) {
            BddNode x = apply(l.low, r, opr);
            BddNode y = apply(l.high, r, opr);
            res = mk(l.level, x, y);
        } else {
            BddNode x = apply(l, r.low, opr);
            BddNode y = apply(r, r.high, opr);
            res = mk(l.level, x, y);
        }
        opCache.put(opr, l, r, res);
        return res;
    }

    public static BddNode relProd(BddNode l, BddNode r, BddNode var) {
        return appex(l, r, Operation.AND, var);
    }

    /**
     * Apply operation and existential quantification
     *
     * @param l   the left argument
     * @param r   the right argument
     * @param opr the operation
     * @param var the BDD variable being quantified
     * @return the result of applying operation
     * @see "BDD buddy.bdd_appex(BDD left, BDD right, int opr, BDD var)"
     */
    public static BddNode appex(BddNode l, BddNode r, byte opr, BddNode var) {
        // TODO: verify that the operator is correct
        if (isTerminal(var)) {
            return apply(l, r, opr);
        }
        Quant quant = varset2vartable(var);
        return opr == Operation.AND ? relProd_rec(l, r, quant, Operation.OR) : appquant_rec(l, r, quant, Operation.OR, opr);
    }

    private static BddNode relProd_rec(final BddNode l, final BddNode r, final Quant quant, byte opr) {
        if (l == ZERO || r == ZERO) {
            return ZERO;
        }
        if (l == r) {
            return quant_rec(l, quant, opr);
        }
        if (l == ONE) {
            return quant_rec(r, quant, opr);
        }
        if (r == ONE) {
            return quant_rec(l, quant, opr);
        }
        byte levelL = l.level;
        byte levelR = r.level;
        BddNode res;
        if (levelL > quant.last && levelR > quant.last) {
            // TODO: in JavaBDD applyop is modified and then restored back. Is that necessary?
            res = and(l, r);
        } else {
            BddNode cached = opCache.get(Operation.RELPROD, l, r);
            if (cached != null) {
                return cached;
            }
            if (levelL == levelR) {
                BddNode x = relProd_rec(l.low, r.low, quant, opr);
                BddNode y = relProd_rec(l.high, r.high, quant, opr);
                if (quant.INVARSET(levelL)) {
                    res = or(x, y);
                } else {
                    res = mk(levelL, x, y);
                }
            } else if (levelL < levelR) {
                BddNode x = relProd_rec(l.low, r, quant, opr);
                BddNode y = relProd_rec(l.high, r, quant, opr);
                if (quant.INVARSET(levelL)) {
                    res = or(x, y);
                } else {
                    res = mk(levelL, x, y);
                }
            } else {
                BddNode x = relProd_rec(l, r.low, quant, opr);
                BddNode y = relProd_rec(l, r.high, quant, opr);
                if (quant.INVARSET(levelR)) {
                    res = or(x, y);
                } else {
                    res = mk(levelR, x, y);
                }
            }
            opCache.put(Operation.RELPROD, l, r, res);
        }
        return res;
    }

    public static BddNode appquant_rec(BddNode l, BddNode r, Quant quant, byte applyop, byte appexop) {
        if (appexop == Operation.AND) {
            throw new BddException();
        }
        switch (appexop) {
            case Operation.OR:
                if (l == ONE || r == ONE) {
                    return ONE;
                }
                if (l == r) {
                    return quant_rec(l, quant, applyop);
                }
                if (l == ZERO) {
                    return quant_rec(r, quant, applyop);
                }
                if (r == ZERO) {
                    return quant_rec(l, quant, applyop);
                }
                break;
            case Operation.XOR:
                if (l == r) {
                    return ZERO;
                }
                if (l == ZERO) {
                    return quant_rec(r, quant, applyop);
                }
                if (r == ZERO) {
                    return quant_rec(l, quant, applyop);
                }
                break;
            case Operation.NAND:
                if (l == ZERO || r == ZERO) {
                    return ONE;
                }
                break;
            case Operation.NOR:
                if (l == ONE || r == ONE) {
                    return ZERO;
                }
                break;
        }
        BddNode res;
        int lNumber = terminalToNumber(l);
        int rNumber = terminalToNumber(r);
        if (isTerminal(lNumber) && isTerminal(rNumber)) {
            res = OPR_RES[applyop][lNumber << 1 | rNumber];
        } else if (l.level > quant.last && r.level > quant.last) {
            res = apply(l, r, appexop);
        } else {
            byte levelL = l.level;
            byte levelR = r.level;
            // TODO: cache lookup
            if (levelL == levelR) {
                BddNode x = appquant_rec(l.low, r.low, quant, applyop, appexop);
                BddNode y = appquant_rec(l.high, r.high, quant, applyop, appexop);
                if (quant.INVARSET(levelL)) {
                    res = apply(x, y, applyop);
                } else {
                    res = mk(levelL, x, y);
                }
            } else if (levelL < levelR) {
                BddNode x = appquant_rec(l.low, r, quant, applyop, appexop);
                BddNode y = appquant_rec(l.high, r, quant, applyop, appexop);
                if (quant.INVARSET(levelL)) {
                    res = apply(x, y, applyop);
                } else {
                    res = mk(levelL, x, y);
                }
            } else {
                BddNode x = appquant_rec(l, r.low, quant, applyop, appexop);
                BddNode y = appquant_rec(l, r.high, quant, applyop, appexop);
                if (quant.INVARSET(levelR)) {
                    res = apply(x, y, applyop);
                } else {
                    res = mk(levelR, x, y);
                }
            }
        }
        // TODO: cache insert
        return res;
    }

    /**
     * Existential quantification of variables.
     *
     * @param r   the argument function
     * @param var the variable
     * @return the result of applying the operation
     * @see "BDD buddy.bdd_exist(BDD r, BDD var)"
     */
    public static BddNode exist(BddNode r, BddNode var) {
        if (isTerminal(var)) {
            return r;
        }
        Quant quant = varset2vartable(var);
        return quant_rec(r, quant, Operation.OR);
    }

    private static BddNode quant_rec(final BddNode r, final Quant quant, byte applyop) {
        if (isTerminal(r) || r.level > quant.last) {
            return r;
        }
        //TODO: the cached valued has to the depend on 'quant', too!
        BddNode cached = opCache.get(Operation.QUANT, r, null);
        if (cached != null) {
            return cached;
        }
        BddNode x = quant_rec(r.low, quant, applyop);
        BddNode y = quant_rec(r.high, quant, applyop);
        BddNode res;
        if (quant.INVARSET(r.level)) {
            if (applyop == Operation.AND) {
                res = and(x, y);
            } else if (applyop == Operation.OR) {
                res = or(x, y);
            } else {
                res = apply(x, y, applyop);
            }
        } else {
            res = mk(r.level, x, y);
        }
        opCache.put(Operation.QUANT, r, null, res);
        return res;
    }

    private static Quant varset2vartable(BddNode r) {
        if (isTerminal(r)) {
            throw new BddException();
        }
        return new Quant(r);
    }

    static class Quant {
        boolean[] varset;
        byte last;

        Quant(BddNode r) {
            varset = new boolean[numberOfVariables];
            last = -1;
            while (!isTerminal(r)) {
                varset[r.level] = true;
                last = r.level;
                r = r.high;
            }
        }

        private boolean INVARSET(int a) {
            return varset[a];
        }
    }

    private static boolean isTerminal(BddNode node) {
        return node == ZERO || node == ONE;
    }

    private static boolean isTerminal(int hashCode) {
        return hashCode == 0 || hashCode == 1;
    }

    private static int terminalToNumber(BddNode terminal) {
        return terminal.hash;
    }

    /**
     * encodes the set of variables as a BDD
     *
     * @param varset
     * @return a BDD
     * @see "buddy.bdd_makeset"
     */
    public static BddNode makeSet(int[] varset) {
        BddNode res = ONE;
        int varnum = varset.length;
        for (int v = 0; v < varnum; v++) {
            res = and(res, ithVar(varset[v]));
        }
        return res;
    }

    public static BddNode ithVar(int n) {
        return varList[n];
    }

    public static BddNode nithVar(int n) {
        return nvarList[n];
    }

    public static int level2Var(int n) {
        return n;
    }

    public static int var2Level(int n) {
        return n;
    }

    /**
     * @return the number of variables in the BDD
     */
    public static int getNumVars() {
        return numberOfVariables;
    }
}
