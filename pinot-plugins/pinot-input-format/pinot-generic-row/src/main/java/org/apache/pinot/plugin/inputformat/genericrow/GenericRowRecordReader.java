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
package org.apache.pinot.plugin.inputformat.genericrow;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;


/**
 * Record reader for file containing serialized GenericRow objects
 */
public class GenericRowRecordReader implements RecordReader {
  private File _dataFile;
  private ObjectInputStream _genericRowInputStream;
  private GenericRowRecordExtractor _recordExtractor;
  private GenericRow _nextRow;

  public GenericRowRecordReader() {
  }

  private void init()
      throws IOException {
    _genericRowInputStream = new ObjectInputStream(new FileInputStream(_dataFile));
    _nextRow = null;
  }

  @Override
  public void init(File dataFile, Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig)
      throws IOException {
    _dataFile = dataFile;
    _recordExtractor = new GenericRowRecordExtractor();
    _recordExtractor.init(fieldsToRead, null);
    init();
  }

  @Override
  public boolean hasNext() {
    if (_nextRow != null) {
      return true;
    }
    try {
      _nextRow = (GenericRow) _genericRowInputStream.readObject();
      return true;
    } catch (EOFException e) {
      return false;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public GenericRow next() {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow reuse) {
    if (_nextRow == null) {
      try {
        _nextRow = (GenericRow) _genericRowInputStream.readObject();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    GenericRow next = _recordExtractor.extract(_nextRow, reuse);
    _nextRow = null;
    return next;
  }

  @Override
  public void rewind()
      throws IOException {
    _genericRowInputStream.close();
    init();
  }

  @Override
  public void close()
      throws IOException {
    _genericRowInputStream.close();
  }
}
