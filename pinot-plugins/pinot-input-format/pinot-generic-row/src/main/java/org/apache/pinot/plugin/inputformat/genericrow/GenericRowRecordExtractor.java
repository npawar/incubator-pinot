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

import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.BaseRecordExtractor;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;


/**
 * Extractor for GenericRow records
 */
public class GenericRowRecordExtractor extends BaseRecordExtractor<GenericRow> {

  private Set<String> _fields;

  @Override
  public void init(Set<String> fields, @Nullable RecordExtractorConfig recordExtractorConfig) {
    if (fields == null || fields.isEmpty()) {
      _fields = Collections.emptySet();
    } else {
      _fields = ImmutableSet.copyOf(fields);
    }
  }

  @Override
  public GenericRow extract(GenericRow from, GenericRow to) {
    Set<String> fieldNames;
    if (_fields.isEmpty()) {
      fieldNames = from.getFieldToValueMap().keySet();
    } else {
      fieldNames = _fields;
    }

    for (String fieldName : fieldNames) {
      Object value = from.getValue(fieldName);
      if (value != null) {
        value = convert(value);
      }
      to.putValue(fieldName, value);
    }
    return to;
  }

  @Override
  public boolean isMultiValue(Object value) {
    return super.isMultiValue(value) || value instanceof Object[];
  }

  @Nullable
  @Override
  protected Object convertMultiValue(Object value) {
    if (value instanceof Collection) {
      return super.convertMultiValue(value);
    }
    Object[] collection = (Object[]) value;
    int numValues = collection.length;
    Object[] array = new Object[numValues];
    int index = 0;
    for (Object element : collection) {
      Object convertedValue = null;
      if (element != null) {
        convertedValue = convert(element);
      }
      if (convertedValue != null && !convertedValue.toString().equals("")) {
        array[index++] = convertedValue;
      }
    }

    if (index == numValues) {
      return array;
    } else if (index == 0) {
      return null;
    } else {
      return Arrays.copyOf(array, index);
    }
  }
}
