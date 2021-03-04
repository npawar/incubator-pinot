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
package org.apache.pinot.spi.ingestion.segment.writer;

import java.io.Closeable;
import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * An interface to write records into Pinot.
 * This interface helps abstract out details regarding segment generation and push from the caller.
 */
public interface SegmentWriter extends Closeable {

  /**
   * Initialize the {@link SegmentWriter} with details about the Pinot cluster and table
   */
  void init(PinotConfiguration conf)
      throws IOException, URISyntaxException;

  /**
   * Collect a single {@link GenericRow} into a buffer.
   * This row is not available in Pinot until a <code>flush()</code> is invoked.
   */
  void collect(GenericRow row)
      throws IOException;

  /**
   * Collect a batch of {@link GenericRow}s into a buffer.
   * These rows are not available in Pinot until a <code>flush()</code> is invoked.
   */
  void collect(GenericRow[] rowBatch)
      throws IOException;

  /**
   * Creates one Pinot segment using the {@link GenericRow}s collected in the buffer, and uploads the segment to Pinot.
   * Successful invocation of this method means that the {@link GenericRow}s collected so far,
   * are now available in Pinot and not available in the buffer anymore.
   */
  void flush()
      throws IOException;
}
