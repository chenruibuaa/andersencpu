package galois.objects.graph;

interface EdgeData<E> {
  void setDouble(long idx, double v);
  void setLong(long idx, long v);
  void setInt(long idx, int v);
  void setFloat(long idx, float v);
  void setObject(long idx, E v);

  double getDouble(long idx);
  long getLong(long idx);
  int getInt(long idx);
  float getFloat(long idx);
  E getObject(long idx);

  Object get(long idx);
  void set(long idx, Object v);
}
