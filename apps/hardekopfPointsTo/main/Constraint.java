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

 File: Constraint.java
 */

package hardekopfPointsTo.main;

import galois.objects.Mappables;
import galois.runtime.GaloisRuntime;
import util.fn.LambdaVoid;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

/**
 * There are 5 types of constraints in Andersen's analysis:
 * Address-of (Base): D = &S
 * Copy (Simple): D = S
 * Load (Complex 1): D = *S + off
 * Store (Complex 2): *D + off = S
 * GEP (copy+offset): D = S + off
 */

public class Constraint {

    static Constraint[] constraints;

    public static final byte ADDR_OF = 0;
    public static final byte COPY = 1;
    public static final byte LOAD = 2;
    public static final byte STORE = 3;
    public static final byte GEP = 4;

    public final byte type;
    // index within the list of constraints, which might change (because constraints are removed
    // during the offline phase)
    int src, dst;
    final int offset;

    public Constraint(int src, int dest, int offset, byte type) {
        this.src = src;
        this.dst = dest;
        this.type = type;
        this.offset = offset;
    }

    @Override
    public int hashCode() {
        int result = 527 + src;
        result = 31 * result + dst;
        result = 31 * result + offset;
        return 31 * result + type;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        Constraint other = (Constraint) o;
        return src == other.src && dst == other.dst && type == other.type && offset == other.offset;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{src = ").append(src).append(", dst = ").append(dst).append(", offset = ").append(offset)
                .append(", type = ").append(type).append(" }");
        return sb.toString();
    }

    public static Constraint[] readConstraints(String filename, final int numNodes) throws Exception {
        BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(filename))));
        String strLine = br.readLine();
        boolean newFormat = strLine.startsWith("#");
        int numConstraints = 0;
        if (newFormat) {
            // second line: number of constraints of each type, separated by comma
            strLine = br.readLine();
            String[] info = strLine.split(",");
            for (String i : info) {
                numConstraints += Integer.parseInt(i);
            }
        } else {
            numConstraints = Integer.parseInt(strLine);
        }
        constraints = new Constraint[numConstraints];
        Statistics.numConstraints = numConstraints;
        GaloisRuntime.forall(Mappables.fromReader(br), new LambdaVoid<String>() {
            @Override
            public void call(String line) {
                if (line.startsWith("#") || line.isEmpty()) {
                    // ignore comments and blank lines
                    return;
                }
                String[] info = line.split(",");
                int id = Integer.parseInt(info[0]);
                int srcId = Integer.parseInt(info[1]);
                if (srcId >= numNodes || srcId < 0) {
                    throw new RuntimeException("Invalid src id for the constraint : " + srcId);
                }
                int dstId = Integer.parseInt(info[2]);
                if (dstId >= numNodes || dstId < 0) {
                    throw new RuntimeException("Invalid dst id for the constraint: " + dstId);
                }
                byte type = Byte.parseByte(info[3]);
                Statistics.initialConstraintCount[type].incrementAndGet();
                int offset = Integer.parseInt(info[4]);
                Constraint cons = new Constraint(srcId, dstId, offset, type);
                constraints[id] = cons;
            }
        });
        return constraints;
    }
}
