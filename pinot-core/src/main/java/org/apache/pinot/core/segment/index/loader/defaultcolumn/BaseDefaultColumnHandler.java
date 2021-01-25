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
package org.apache.pinot.core.segment.index.loader.defaultcolumn;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.core.segment.creator.ColumnIndexCreationInfo;
import org.apache.pinot.core.segment.creator.TextIndexType;
import org.apache.pinot.core.segment.creator.impl.SegmentColumnarIndexCreator;
import org.apache.pinot.core.segment.creator.impl.SegmentDictionaryCreator;
import org.apache.pinot.core.segment.creator.impl.V1Constants;
import org.apache.pinot.core.segment.creator.impl.fwd.MultiValueUnsortedForwardIndexCreator;
import org.apache.pinot.core.segment.creator.impl.fwd.SingleValueSortedForwardIndexCreator;
import org.apache.pinot.core.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.core.segment.store.SegmentDirectory;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.ByteArray;


public abstract class BaseDefaultColumnHandler implements DefaultColumnHandler {

  protected final File _indexDir;
  protected final Schema _schema;
  protected final SegmentMetadataImpl _segmentMetadata;
  protected final SegmentDirectory.Writer _segmentWriter;
  protected final IndexLoadingConfig _indexLoadingConfig;

  private final PropertiesConfiguration _segmentProperties;

  protected BaseDefaultColumnHandler(File indexDir, Schema schema, SegmentMetadataImpl segmentMetadata,
      SegmentDirectory.Writer segmentWriter, IndexLoadingConfig indexLoadingConfig) {
    _indexDir = indexDir;
    _schema = schema;
    _segmentMetadata = segmentMetadata;
    _segmentWriter = segmentWriter;
    _indexLoadingConfig = indexLoadingConfig;
    _segmentProperties = SegmentMetadataImpl.getPropertiesConfiguration(indexDir);
  }

  /**
   * Helper method to remove the V1 indices (dictionary and forward index) for a column.
   *
   * @param column column name.
   */
  protected void removeColumnV1Indices(String column)
      throws IOException {
    // Delete existing dictionary and forward index
    FileUtils.forceDelete(new File(_indexDir, column + V1Constants.Dict.FILE_EXTENSION));
    File svFwdIndex = new File(_indexDir, column + V1Constants.Indexes.SORTED_SV_FORWARD_INDEX_FILE_EXTENSION);
    if (svFwdIndex.exists()) {
      FileUtils.forceDelete(svFwdIndex);
    } else {
      FileUtils.forceDelete(new File(_indexDir, column + V1Constants.Indexes.UNSORTED_MV_FORWARD_INDEX_FILE_EXTENSION));
    }

    // Remove the column metadata
    SegmentColumnarIndexCreator.removeColumnMetadataInfo(_segmentProperties, column);
  }

  /**
   * Helper method to create the V1 indices (dictionary and forward index) for a column.
   *
   * @param column column name.
   */
  protected void createColumnV1Indices(String column)
      throws Exception {
    FieldSpec fieldSpec = _schema.getFieldSpecFor(column);
    Preconditions.checkNotNull(fieldSpec);

    // Generate column index creation information.
    int totalDocs = _segmentMetadata.getTotalDocs();
    DataType dataType = fieldSpec.getDataType();
    Object defaultValue = fieldSpec.getDefaultNullValue();
    boolean isSingleValue = fieldSpec.isSingleValueField();
    int maxNumberOfMultiValueElements = isSingleValue ? 0 : 1;
    int dictionaryElementSize = 0;

    Object sortedArray;
    switch (dataType) {
      case INT:
        Preconditions.checkState(defaultValue instanceof Integer);
        sortedArray = new int[]{(Integer) defaultValue};
        break;
      case LONG:
        Preconditions.checkState(defaultValue instanceof Long);
        sortedArray = new long[]{(Long) defaultValue};
        break;
      case FLOAT:
        Preconditions.checkState(defaultValue instanceof Float);
        sortedArray = new float[]{(Float) defaultValue};
        break;
      case DOUBLE:
        Preconditions.checkState(defaultValue instanceof Double);
        sortedArray = new double[]{(Double) defaultValue};
        break;
      case STRING:
        Preconditions.checkState(defaultValue instanceof String);
        String stringDefaultValue = (String) defaultValue;
        // Length of the UTF-8 encoded byte array.
        dictionaryElementSize = StringUtil.encodeUtf8(stringDefaultValue).length;
        sortedArray = new String[]{stringDefaultValue};
        break;
      case BYTES:
        Preconditions.checkState(defaultValue instanceof byte[]);
        dictionaryElementSize = ((byte[]) defaultValue).length;
        // Convert byte[] to ByteArray for internal usage
        ByteArray bytesDefaultValue = new ByteArray((byte[]) defaultValue);
        defaultValue = bytesDefaultValue;
        sortedArray = new ByteArray[]{bytesDefaultValue};
        break;
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + dataType + " for column: " + column);
    }
    DefaultColumnStatistics columnStatistics =
        new DefaultColumnStatistics(defaultValue  /* min */, defaultValue  /* max */, sortedArray, isSingleValue,
            totalDocs, maxNumberOfMultiValueElements);

    ColumnIndexCreationInfo columnIndexCreationInfo =
        new ColumnIndexCreationInfo(columnStatistics, true/*createDictionary*/, false, true/*isAutoGenerated*/,
            defaultValue/*defaultNullValue*/);

    // Create dictionary.
    // We will have only one value in the dictionary.
    try (SegmentDictionaryCreator creator = new SegmentDictionaryCreator(sortedArray, fieldSpec, _indexDir, false)) {
      creator.build();
    }

    // Create forward index.
    if (isSingleValue) {
      // Single-value column.

      try (SingleValueSortedForwardIndexCreator svFwdIndexCreator = new SingleValueSortedForwardIndexCreator(_indexDir,
          fieldSpec.getName(), 1/*cardinality*/)) {
        for (int docId = 0; docId < totalDocs; docId++) {
          svFwdIndexCreator.putDictId(0);
        }
      }
    } else {
      // Multi-value column.

      try (
          MultiValueUnsortedForwardIndexCreator mvFwdIndexCreator = new MultiValueUnsortedForwardIndexCreator(_indexDir,
              fieldSpec.getName(), 1/*cardinality*/, totalDocs/*numDocs*/, totalDocs/*totalNumberOfValues*/)) {
        int[] dictIds = {0};
        for (int docId = 0; docId < totalDocs; docId++) {
          mvFwdIndexCreator.putDictIdMV(dictIds);
        }
      }
    }

    // Add the column metadata information to the metadata properties.
    SegmentColumnarIndexCreator
        .addColumnMetadataInfo(_segmentProperties, column, columnIndexCreationInfo, totalDocs, fieldSpec,
            true/*hasDictionary*/, dictionaryElementSize, true/*hasInvertedIndex*/, TextIndexType.NONE);
  }
}
