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


*/

package util.concurrent;

/**
 * Concurrent single producer single consumer FIFO.
 * @param <T>
 */
public class ConcurrentSPCQueue<T> {
  private final Object[] buffer;
  private final int mask;
  private volatile int widx;
  private int ridx;

  public ConcurrentSPCQueue(int maxSize) {
    int logMax = 32 - Integer.numberOfLeadingZeros(maxSize - 1);
    mask = (1 << logMax) - 1;
    buffer = new Object[1 << logMax];
    widx = 0;
    ridx = 0;
  }

  public boolean add(T item) {
    if (buffer[widx & mask] == null) {
      buffer[widx++ & mask] = item;
      return true;
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  public T poll() {
    // Spin here to protect against race in add between 
    // incrementing writerIndex and storing value
    int index = ridx & mask;
    Object v = null;
    do {
      if ((ridx & mask) == (widx & mask))
        return null;
    } while ((v = buffer[index]) == null);

    buffer[index] = null;
    ridx++;
    
    return (T) v;
  }
}
