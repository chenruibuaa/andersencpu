package galois.objects.graph;

abstract class AbstractEdgeData<E> implements EdgeData<E> {
  @Override
  public void setDouble(long idx, double v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setLong(long idx, long v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setInt(long idx, int v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setFloat(long idx, float v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setObject(long idx, E v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDouble(long idx) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLong(long idx) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getInt(long idx) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloat(long idx) {
    throw new UnsupportedOperationException();
  }

  @Override
  public E getObject(long idx) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void set(long idx, Object v) {
    throw new UnsupportedOperationException();
  }
}
