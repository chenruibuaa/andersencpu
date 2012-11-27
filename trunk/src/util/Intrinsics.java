/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

package util;


public class Intrinsics {
  private static final sun.misc.Unsafe UNSAFE = getUnsafe();
  public static boolean compareAndSetInt(Object obj, long offset, int expect, int update) {
    return UNSAFE.compareAndSwapInt(obj, offset, expect, update);
  }

  public static boolean compareAndSetObject(Object obj, long offset, Object expect, Object update) {
    return UNSAFE.compareAndSwapObject(obj, offset, expect, update);
  }
  
  public static boolean compareAndSetLong(Object obj, long offset, long expect, long update) {
    return UNSAFE.compareAndSwapLong(obj, offset, expect, update);
  }

  public static long objectFieldOffset(String field, Class<?> klazz) {
    try {
      return UNSAFE.objectFieldOffset(klazz.getDeclaredField(field));
    } catch (NoSuchFieldException e) {
      // Convert Exception to corresponding Error
      NoSuchFieldError error = new NoSuchFieldError(field);
      error.initCause(e);
      throw error;
    }
  }

  /**
   * Returns a sun.misc.Unsafe. Suitable for use in a 3rd party package. Replace
   * with a simple call to Unsafe.getUnsafe when integrating into a jdk.
   * 
   * @return a sun.misc.Unsafe
   */
  private static sun.misc.Unsafe getUnsafe() {
    try {
      return sun.misc.Unsafe.getUnsafe();
    } catch (SecurityException se) {
      try {
        return java.security.AccessController
            .doPrivileged(new java.security.PrivilegedExceptionAction<sun.misc.Unsafe>() {
              public sun.misc.Unsafe run() throws Exception {
                java.lang.reflect.Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
                f.setAccessible(true);
                return (sun.misc.Unsafe) f.get(null);
              }
            });
      } catch (java.security.PrivilegedActionException e) {
        throw new RuntimeException("Could not initialize intrinsics", e.getCause());
      }
    }
  }
}
