package util.concurrent;

import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * {@link AtomicIntegerArray} on a bit-basis. This potentially introduces more
 * false sharing at the expense of a more compact representation.
 */
public class AtomicBitArray {
  private static final int LOG2_SIZE_INT = 5;
  private static final int SIZE_INT_MASK = (1 << 5) - 1;

  private final AtomicIntegerArray array;
  private final int length;

  public AtomicBitArray(int length) {
    this.length = length;
    array = new AtomicIntegerArray((length >> LOG2_SIZE_INT) + 1);
  }

  public boolean compareAndSet(int i, boolean expect, boolean update) {
    int index = i >> LOG2_SIZE_INT;
    int offset = i & SIZE_INT_MASK;
    int value = array.get(index);
    
    boolean orig = getBit(value, offset);
    
    if (orig != expect)
      return false;
    
    int next = setBit(value, offset, update);
    
    return array.compareAndSet(index, value, next);
  }

  private static boolean getBit(int value, int offset) {
    return (value & (1 << offset)) != 0;
  }

  private static int setBit(int value, int offset, boolean v) {
    if (v)
      return value | (1 << offset);
    else
      return value & ~(1 << offset);
  }

  public boolean get(int i) {
    int index = i >> LOG2_SIZE_INT;
    int offset = i & SIZE_INT_MASK;
    int value = array.get(index);

    return getBit(value, offset);
  }

  public void set(int i, boolean v) {
    int index = i >> LOG2_SIZE_INT;
    int offset = i & SIZE_INT_MASK;
    int value = array.get(index);

    array.set(index, setBit(value, offset, v));
  }

  public int length() {
    return length;
  }
}
