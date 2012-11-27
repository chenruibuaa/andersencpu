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

File: ListMappable.java 

 */

package galois.objects;

import galois.runtime.PmapContext;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import util.fn.Lambda2Void;
import util.fn.Lambda3Void;
import util.fn.LambdaVoid;

class ListMapper<T> implements Mappable<T> {
  private final List<T> list;
  private final int chunkSize;
  private final int size;

  public ListMapper(List<T> list, int chunkSize) {
    this.list = list;
    this.chunkSize = chunkSize;
    size = list.size();
  }

  @Override
  public void pmap(LambdaVoid<T> body, PmapContext ctx) {
    AtomicInteger cur = (AtomicInteger) ctx.getContextObject();
    for (int i = cur.getAndAdd(chunkSize); i < size; i = cur.getAndAdd(chunkSize)) {
      for (int j = 0; j < chunkSize; j++) {
        int index = i + j;
        if (index >= size)
          break;

        T item = list.get(index);
        body.call(item);
      }
    }
  }

  @Override
  public void beforePmap(PmapContext ctx) {
    ctx.setContextObject(new AtomicInteger());
  }

  @Override
  public void afterPmap(PmapContext ctx) {
  }
  
  @Override
  public final void map(LambdaVoid<T> body) {
    map(body, MethodFlag.ALL);
  }

  @Override
  public void map(LambdaVoid<T> body, byte flags) {
    for (int i = 0; i < size; i++) {
      body.call(list.get(i));
    }
  }

  @Override
  public final <A1> void map(Lambda2Void<T, A1> body, A1 arg1) {
    map(body, arg1, MethodFlag.ALL);
  }

  @Override
  public <A1> void map(Lambda2Void<T, A1> body, A1 arg1, byte flags) {
    for (int i = 0; i < size; i++) {
      body.call(list.get(i), arg1);
    }
  }

  @Override
  public final <A1, A2> void map(Lambda3Void<T, A1, A2> body, A1 arg1, A2 arg2) {
    map(body, arg1, arg2, MethodFlag.ALL);
  }

  @Override
  public <A1, A2> void map(Lambda3Void<T, A1, A2> body, A1 arg1, A2 arg2, byte flags) {
    for (int i = 0; i < size; i++) {
      body.call(list.get(i), arg1, arg2);
    }
  }
}
