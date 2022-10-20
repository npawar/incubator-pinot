/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.spi;

import java.util.Set;
import org.apache.pinot.segment.spi.store.ColumnIndexType;


/**
 * Contains the column specific context within {@link FetchContext}
 */
public class FetchColumnContext {
  private final boolean _fetchAll;
  private final Set<ColumnIndexType> _fetch;
  private final Set<ColumnIndexType> _prefetch;

  public FetchColumnContext(boolean fetchAll, Set<ColumnIndexType> fetch, Set<ColumnIndexType> prefetch) {
    _fetchAll = fetchAll;
    _fetch = fetch;
    _prefetch = prefetch;
  }

  /**
   * Whether to fetch all {@link ColumnIndexType} for this column
   */
  public boolean isFetchAll() {
    return _fetchAll;
  }

  /**
   * A specific set of {@link ColumnIndexType} to fetch for this column
   */
  public Set<ColumnIndexType> getFetch() {
    return _fetch;
  }

  /**
   * A subset of columns to fetch, which are marked for prefetch based on query
   */
  public Set<ColumnIndexType> getPrefetch() {
    return _prefetch;
  }
}
