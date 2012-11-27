package galois.runtime.wl;

import java.util.Comparator;

import util.fn.Lambda;

public enum FunctionParameterType {
  LAMBDA_T_INT_1(0, 1),
  LAMBDA_T_FLOAT_1(1, 1),
  COMPARATOR_0(2, 0);
  
  private final int origType;
  private final int position;
  FunctionParameterType(int origType, int position) {
    this.origType = origType;
    this.position = position;
  }
  
  public int getPosition() {
    return position;
  }
  
  public <T,U> Object compose(final Lambda<U,T> wrapper, final Object orig) {
    switch (origType) {
    case 0: return new Lambda<U,Integer>() {
      @SuppressWarnings("unchecked")
      @Override
      public Integer call(U arg0) {
        return ((Lambda<T,Integer>) orig).call(wrapper.call(arg0));
      }
    };
    case 1: return new Lambda<U,Float>() {
      @SuppressWarnings("unchecked")
      @Override
      public Float call(U arg0) {
        return ((Lambda<T,Float>) orig).call(wrapper.call(arg0));
      }
    };
    case 2: return new Comparator<U>() {
      @SuppressWarnings("unchecked")
      @Override
      public int compare(U o1, U o2) {
        return ((Comparator<T>) orig).compare(wrapper.call(o1), wrapper.call(o2));
      }
    };
    default:
      throw new Error("Unknown target function type");
    }
  }
}
