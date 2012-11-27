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

File: Mappables.java 

 */

package galois.objects;

import galois.runtime.GaloisRuntime;

import java.io.BufferedReader;
import java.util.List;

import util.Pair;
import util.fn.Lambda;

/**
 * Several helper methods to bridge common Java classes into {@link Mappable}
 * framework. 
 * 
 *
 */
public class Mappables {
  /**
   * Forms the product space between two {@link Mappable}s. This is useful for
   * flattening nested iterators into a single iterator. Product iteration
   * proceeds by row, completing an entire row before moving on to the next
   * row. During concurrent execution, entire rows are processed concurrently.
   * 
   * The pair objects produced by this mappable are reused between iterations.
   * Therefore, it is an error to store references to the argument pairs;
   * make a copy instead. 
   *
   * @param <A>   type of first component of the product space
   * @param <B>   type of second component of the product space
   * @param rows  mappable that iterates over first component of the product space
   * @param cols  function that generates a mappable over the second component of the product
   *              space from an element of the first component
   * @return      a mappable over the product space
   */
  public static <A, B> Mappable<Pair<A, B>> product(Mappable<A> rows, Lambda<A, Mappable<B>> cols) {
    return new ProductMapper<A, B>(rows, cols);
  }

  /**
   * Creates a mappable over lines of a stream. Once iteration is complete, the
   * stream is closed and further use of the mappable will be invalid.
   * 
   * @param reader  reader of stream
   * @return        a mappable instance over lines of a stream
   */
  public static Mappable<String> fromReader(BufferedReader reader) {
    return new LineMapper(reader);
  }

  /**
   * Creates a mappable over integers in a range. 
   * 
   * @param start     starting integer
   * @param end       ending integer (exclusive)
   * @return          a mappable over [start, end)
   */
  public static Mappable<Integer> range(int start, int end) {
    int chunkSize = 2 * GaloisRuntime.getRuntime().getMaxThreads();
    return range(start, end, chunkSize);
  }

  /**
   * Creates a mappable over integers in a range. Integers are
   * given out in <code>chunkSize</code> size blocks. 
   * 
   * @param start     starting integer
   * @param end       ending integer (exclusive)
   * @param chunkSize the chunk size
   * @return          a mappable over [start, end)
   */
  public static Mappable<Integer> range(int start, int end, int chunkSize) {
    return new RangeMapper(start, end, chunkSize);
  }

  public static <T> Mappable<T> fromList(final List<T> list) {
    int chunkSize = 2 * GaloisRuntime.getRuntime().getMaxThreads();
    return fromList(list, chunkSize);
  }

  /**
   * Creates a mappable view from a list. Behavior is undefined if the backing list
   * is modified during iteration.
   * 
   * @param <T>   type of elements of the list
   * @param list  list that this mappable is a view of
   * @return      a mappable view of the given list
   */
  public static <T> Mappable<T> fromList(List<T> list, int chunkSize) {
    return new ListMapper<T>(list, chunkSize);
  }
}
