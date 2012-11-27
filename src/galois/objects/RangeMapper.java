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

File: RangeMapper.java 

 */

package galois.objects;

import galois.runtime.PmapContext;

import java.util.concurrent.atomic.AtomicInteger;

import util.fn.Lambda2Void;
import util.fn.Lambda3Void;
import util.fn.LambdaVoid;

class RangeMapper implements Mappable<Integer> {
  private final int start;
  private final int end;
  private final int chunkSize;

  public RangeMapper(int start, int end, int chunkSize) {
    this.start = start;
    this.end = end;
    this.chunkSize = chunkSize;
  }

  @Override
  public void pmap(LambdaVoid<Integer> body, PmapContext ctx) {
    AtomicInteger cur = (AtomicInteger) ctx.getContextObject();
    for (int i = cur.getAndAdd(chunkSize); i < end; i = cur.getAndAdd(chunkSize)) {
      for (int j = 0; j < chunkSize; j++) {
        int index = i + j;
        if (index >= end)
          break;

        body.call(index);
      }
    }
  }

  @Override
  public void beforePmap(PmapContext ctx) {
    ctx.setContextObject(new AtomicInteger(start));
  }
  
  @Override
  public void afterPmap(PmapContext ctx) {
   
  }

  @Override
  public final void map(LambdaVoid<Integer> body) {
    map(body, MethodFlag.ALL);
  }

  @Override
  public void map(LambdaVoid<Integer> body, byte flags) {
    for (int index = start; index < end; index++) {
      body.call(index);
    }
  }

  @Override
  public final <A1> void map(Lambda2Void<Integer, A1> body, A1 arg1) {
    map(body, arg1, MethodFlag.ALL);
  }

  @Override
  public <A1> void map(Lambda2Void<Integer, A1> body, A1 arg1, byte flags) {
    for (int index = start; index < end; index++) {
      body.call(index, arg1);
    }
  }

  @Override
  public final <A1, A2> void map(Lambda3Void<Integer, A1, A2> body, A1 arg1, A2 arg2) {
    map(body, arg1, arg2, MethodFlag.ALL);
  }

  @Override
  public <A1, A2> void map(Lambda3Void<Integer, A1, A2> body, A1 arg1, A2 arg2, byte flags) {
    for (int index = start; index < end; index++) {
      body.call(index, arg1, arg2);
    }
  }
}
