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

import java.util.Map;
import java.util.UUID;
import org.apache.pinot.segment.spi.store.ColumnIndexType;


/**
 * The context for fetching buffers of a segment during query.
 */
public class FetchContext {
  private final UUID _fetchId;
  private final String _segmentName;
  private final Map<String, FetchColumnContext> _fetchColumnContextMap;
  private final Map<String, String> _queryOptions;

  /**
   * Create a new FetchRequest for this segment, to fetch all buffers of the given columns
   * @param fetchId unique fetch id
   * @param segmentName segment name
   * @param fetchColumnContextMap map containing column name to {@link FetchColumnContext},
   *                              indicating which {@link ColumnIndexType} to fetch, and which of them to prefetch
   * @param queryOptions query options for this fetch
   */
  public FetchContext(UUID fetchId, String segmentName, Map<String, FetchColumnContext> fetchColumnContextMap,
      Map<String, String> queryOptions) {
    _fetchId = fetchId;
    _segmentName = segmentName;
    _fetchColumnContextMap = fetchColumnContextMap;
    _queryOptions = queryOptions;
  }

  /**
   * An id to uniquely identify the fetch request
   * @return unique uuid
   */
  public UUID getFetchId() {
    return _fetchId;
  }

  /**
   * Segment name associated with this fetch context
   */
  public String getSegmentName() {
    return _segmentName;
  }

  /**
   * Map containing column name to {@link FetchColumnContext},
   * indicating which {@link ColumnIndexType} to fetch, and which of them to prefetch
   */
  public Map<String, FetchColumnContext> getFetchColumnContextMap() {
    return _fetchColumnContextMap;
  }

  /**
   * Query context for this fetch
   */
  public Map<String, String> getQueryOptions() {
    return _queryOptions;
  }
}
