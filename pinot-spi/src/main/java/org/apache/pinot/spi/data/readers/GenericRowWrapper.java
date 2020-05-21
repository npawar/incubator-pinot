package org.apache.pinot.spi.data.readers;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;


public class GenericRowWrapper {

  private List<GenericRow> _genericRows = new ArrayList<>();
  private final GenericRowPool _genericRowPool = new GenericRowPool();

  public void clear() {
    for (GenericRow genericRow : _genericRows) {
      genericRow.clear();
      _genericRowPool.releaseReusable(genericRow);
    }
    _genericRows = new ArrayList<>();
  }

  public int size() {
    return _genericRows.size();
  }

  public GenericRow getReusableGenericRow() {
    GenericRow reusableGenericRow = _genericRowPool.acquireReusable();
    _genericRows.add(reusableGenericRow);
    return reusableGenericRow;
  }

  public List<GenericRow> getGenericRows() {
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
