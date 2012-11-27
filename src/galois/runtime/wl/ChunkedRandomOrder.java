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





package galois.runtime.wl;

import galois.runtime.ThreadContext;

import java.util.Arrays;
import java.util.Random;

import util.fn.Lambda0;

/**
 * Order elements randomly but consider elements as grouped into chunks of size <i>N</i>.
 * This produces slightly different orders than
 * {@link RandomOrder} because full chunks are selected uniformly at random rather than
 * elements. Incomplete chunks are excluded from this selection and are unordered with
 * respect to each other.
 * 
 *
 * @param <T>  the type of elements of the worklist
 * @see RandomOrder
 */
@OnlyLeaf
@MatchingConcurrentVersion(ConcurrentChunkedRandomOrder.class)
@MatchingLeafVersion(ChunkedRandomOrder.class)
public class ChunkedRandomOrder<T> implements Worklist<T> {
  public static final int DEFAULT_CHUNK_SIZE = 32;
  public static final int DEFAULT_INITIAL_CAPACITY = 1024;

  private final int chunkSize;
  private final Random rand;
  private final int initialCapacity;
  private int queueSize;
  private T queue[];
  private Worklist<T> current;
  private int size;

  /**
   * Creates a chunked random order with the default chunk size ({@value #DEFAULT_CHUNK_SIZE})
   * and default initial capacity ({@value #DEFAULT_INITIAL_CAPACITY})
   */
  public ChunkedRandomOrder(Lambda0<Worklist<T>> maker, boolean needSize) {
    this(DEFAULT_CHUNK_SIZE, DEFAULT_INITIAL_CAPACITY, maker, needSize);
  }

  /**
   * Creates a chunked random order with the given chunk size and
   * default initial capacity ({@value #DEFAULT_INITIAL_CAPACITY})
   * 
   * @param chunkSize  chunk size to use
   */
  public ChunkedRandomOrder(int chunkSize, Lambda0<Worklist<T>> maker, boolean needSize) {
    this(chunkSize, DEFAULT_INITIAL_CAPACITY, maker, needSize);
  }

  /**
   * Creates a chunked random order with the given chunk size and initial capacity
   * 
   * @param chunkSize        chunk size to use
   * @param initialCapacity  initial capacity
   */
  public ChunkedRandomOrder(int chunkSize, int initialCapacity, Lambda0<Worklist<T>> maker, boolean needSize) {
    this(chunkSize, initialCapacity, new BoundedLIFO<T>(chunkSize, null, false), needSize);
  }

  @SuppressWarnings("unchecked")
  private ChunkedRandomOrder(int chunkSize, int initialCapacity, Worklist<T> current, boolean needSize) {
    this.chunkSize = chunkSize;
    this.initialCapacity = initialCapacity;
    this.current = current;

    rand = new Random();
    queue = (T[]) new Object[initialCapacity];
  }

  @Override
  public Worklist<T> newInstance() {
    return new ChunkedRandomOrder<T>(chunkSize, initialCapacity, current.newInstance(), false);
  }

  @Override
  public void add(T item, ThreadContext ctx) {
    size++;
    
    if (current.size() >= chunkSize) {
      T t;
      while ((t = current.poll(ctx)) != null) {
        addInternal(t);
      }
    }

    current.add(item, ctx);
  }

  private void addInternal(T item) {
    if (queueSize + 1 >= queue.length) {
      resize();
    }
    queue[queueSize++] = item;
  }

  @Override
  public T poll(ThreadContext ctx) {
    T retval = current.poll(ctx);

    if (retval == null) {
      for (int i = 0; i < chunkSize + 1; i++) {
        if (queueSize == 0) {
          break;
        }

        int bucket = rand.nextInt(queueSize);
        T item = queue[bucket];
        if (retval == null)
          retval = item;
        else if (current.size() < chunkSize) {
          current.add(item, ctx);
        } else
          break;

        queue[bucket] = queue[queueSize - 1];
        queue[queueSize - 1] = null;
        queueSize--;
      }
    }

    if (retval != null)
      size--;

    return retval;
  }

  @Override
  public T polls() {
    return poll(null);
  }

  @Override
  public int size() {
    return size;
  }

  private void resize() {
    queue = Arrays.copyOf(queue, queue.length * 2);
  }
}
