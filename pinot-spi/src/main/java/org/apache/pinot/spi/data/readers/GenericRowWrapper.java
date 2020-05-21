package org.apache.pinot.spi.data.readers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Stack;


public class GenericRowWrapper {

  private final GenericRow _genericRow = new GenericRow();
  private List<GenericRow> _genericRows = new ArrayList<>();
  private final GenericRowPool _genericRowPool = new GenericRowPool();

  public void clear() {
    _genericRow.clear();
    for (GenericRow genericRow : _genericRows) {
      genericRow.clear();
      _genericRowPool.releaseReusable(genericRow);
    }
    _genericRows = new ArrayList<>();
  }

  public int size() {
    return _genericRows.isEmpty()? 1 : _genericRows.size();
  }

  public void add(GenericRow genericRow) {
    _genericRows.add(genericRow);
  }

  public GenericRow getGenericRow() {
    return _genericRow;
  }

  public GenericRow getReusableGenericRow() {
    return _genericRowPool.acquireReusable();
  }

  public List<GenericRow> getGenericRows() {
    if (_genericRows.isEmpty()) {
      return Collections.singletonList(_genericRow);
    }
    return _genericRows;
  }

  private static class GenericRowPool {

    private final Stack<GenericRow> _pool;

    public GenericRowPool() {
      _pool = new Stack<>();
    }

    public GenericRow acquireReusable() {
      if (_pool.isEmpty()) {
        return new GenericRow();
      }
      return _pool.pop();
    }

    public void releaseReusable(GenericRow genericRow) {
      _pool.add(genericRow);
    }
  }
}
