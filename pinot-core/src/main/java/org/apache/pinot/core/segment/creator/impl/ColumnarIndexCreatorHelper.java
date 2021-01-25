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
package org.apache.pinot.core.segment.creator.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.utils.FileUtils;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.io.compression.ChunkCompressorFactory;
import org.apache.pinot.core.io.writer.impl.BaseChunkSVForwardIndexWriter;
import org.apache.pinot.core.segment.creator.ColumnIndexCreationInfo;
import org.apache.pinot.core.segment.creator.DictionaryBasedInvertedIndexCreator;
import org.apache.pinot.core.segment.creator.ForwardIndexCreator;
import org.apache.pinot.core.segment.creator.TextIndexCreator;
import org.apache.pinot.core.segment.creator.impl.fwd.MultiValueUnsortedForwardIndexCreator;
import org.apache.pinot.core.segment.creator.impl.fwd.SingleValueFixedByteRawIndexCreator;
import org.apache.pinot.core.segment.creator.impl.fwd.SingleValueSortedForwardIndexCreator;
import org.apache.pinot.core.segment.creator.impl.fwd.SingleValueUnsortedForwardIndexCreator;
import org.apache.pinot.core.segment.creator.impl.fwd.SingleValueVarByteRawIndexCreator;
import org.apache.pinot.core.segment.creator.impl.inv.OffHeapBitmapInvertedIndexCreator;
import org.apache.pinot.core.segment.creator.impl.inv.OnHeapBitmapInvertedIndexCreator;
import org.apache.pinot.core.segment.creator.impl.nullvalue.NullValueVectorCreator;
import org.apache.pinot.core.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ColumnarIndexCreatorHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(ColumnarIndexCreatorHelper.class);

  private final Schema _schema;
  private int _docIdCounter;
  private final Map<String, SegmentDictionaryCreator> _dictionaryCreatorMap = new HashMap<>();
  private final Map<String, ForwardIndexCreator> _forwardIndexCreatorMap = new HashMap<>();
  private final Map<String, DictionaryBasedInvertedIndexCreator> _invertedIndexCreatorMap = new HashMap<>();
  private final Map<String, TextIndexCreator> _textIndexCreatorMap = new HashMap<>();
  private final Set<String> _textIndexColumns;
  private final Map<String, NullValueVectorCreator> _nullValueVectorCreatorMap = new HashMap<>();
  private boolean _nullHandlingEnabled;

  public ColumnarIndexCreatorHelper(Schema schema, SegmentGeneratorConfig segmentGeneratorConfig,
      Map<String, ColumnIndexCreationInfo> indexCreationInfoMap, Set<String> invertedIndexColumns,
      Set<String> textIndexColumns, int totalDocs, File indexDir)
      throws Exception {
    _schema = schema;
    _textIndexColumns = textIndexColumns;

    _docIdCounter = 0;
    // Initialize creators for dictionary, forward index and inverted index
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      // Ignore virtual columns
      if (fieldSpec.isVirtualColumn()) {
        continue;
      }

      String columnName = fieldSpec.getName();
      ColumnIndexCreationInfo indexCreationInfo = indexCreationInfoMap.get(columnName);
      Preconditions.checkNotNull(indexCreationInfo, "Missing index creation info for column: %s", columnName);

      if (createDictionaryForColumn(indexCreationInfo, segmentGeneratorConfig, fieldSpec)) {
        // Create dictionary-encoded index

        // Initialize dictionary creator
        SegmentDictionaryCreator dictionaryCreator =
            new SegmentDictionaryCreator(indexCreationInfo.getSortedUniqueElementsArray(), fieldSpec, indexDir,
                indexCreationInfo.isUseVarLengthDictionary());
        _dictionaryCreatorMap.put(columnName, dictionaryCreator);

        // Create dictionary
        try {
          dictionaryCreator.build();
        } catch (Exception e) {
          LOGGER.error("Error building dictionary for field: {}, cardinality: {}, number of bytes per entry: {}",
              fieldSpec.getName(), indexCreationInfo.getDistinctValueCount(), dictionaryCreator.getNumBytesPerEntry());
          throw e;
        }

        // Initialize forward index creator
        int cardinality = indexCreationInfo.getDistinctValueCount();
        if (fieldSpec.isSingleValueField()) {
          if (indexCreationInfo.isSorted()) {
            _forwardIndexCreatorMap
                .put(columnName, new SingleValueSortedForwardIndexCreator(indexDir, columnName, cardinality));
          } else {
            _forwardIndexCreatorMap.put(columnName,
                new SingleValueUnsortedForwardIndexCreator(indexDir, columnName, cardinality, totalDocs));
          }
        } else {
          _forwardIndexCreatorMap.put(columnName,
              new MultiValueUnsortedForwardIndexCreator(indexDir, columnName, cardinality, totalDocs,
                  indexCreationInfo.getTotalNumberOfEntries()));
        }

        // Initialize inverted index creator; skip creating inverted index if sorted
        if (invertedIndexColumns.contains(columnName) && !indexCreationInfo.isSorted()) {
          if (segmentGeneratorConfig.isOnHeap()) {
            _invertedIndexCreatorMap
                .put(columnName, new OnHeapBitmapInvertedIndexCreator(indexDir, columnName, cardinality));
          } else {
            _invertedIndexCreatorMap.put(columnName,
                new OffHeapBitmapInvertedIndexCreator(indexDir, fieldSpec, cardinality, totalDocs,
                    indexCreationInfo.getTotalNumberOfEntries()));
          }
        }
      } else {
        // Create raw index

        // TODO: add support to multi-value column and inverted index
        Preconditions.checkState(fieldSpec.isSingleValueField(), "Cannot create raw index for multi-value column: %s",
            columnName);
        Preconditions.checkState(!invertedIndexColumns.contains(columnName),
            "Cannot create inverted index for raw index column: %s", columnName);

        ChunkCompressorFactory.CompressionType compressionType =
            getColumnCompressionType(segmentGeneratorConfig, fieldSpec);

        // Initialize forward index creator
        boolean deriveNumDocsPerChunk =
            shouldDeriveNumDocsPerChunk(columnName, segmentGeneratorConfig.getColumnProperties());
        int writerVersion = rawIndexWriterVersion(columnName, segmentGeneratorConfig.getColumnProperties());
        _forwardIndexCreatorMap.put(columnName,
            getRawIndexCreatorForColumn(indexDir, compressionType, columnName, fieldSpec.getDataType(), totalDocs,
                indexCreationInfo.getLengthOfLongestEntry(), deriveNumDocsPerChunk, writerVersion));
      }

      if (textIndexColumns.contains(columnName)) {
        // Initialize text index creator
        Preconditions.checkState(fieldSpec.isSingleValueField(),
            "Text index is currently only supported on single-value columns");
        Preconditions.checkState(fieldSpec.getDataType() == DataType.STRING,
            "Text index is currently only supported on STRING type columns");
        _textIndexCreatorMap.put(columnName, new LuceneTextIndexCreator(columnName, indexDir, true /* commitOnClose */));
      }

      _nullHandlingEnabled = segmentGeneratorConfig.isNullHandlingEnabled();
      if (_nullHandlingEnabled) {
        // Initialize Null value vector map
        _nullValueVectorCreatorMap.put(columnName, new NullValueVectorCreator(indexDir, columnName));
      }
    }
  }

  public SegmentDictionaryCreator getDictionaryCreator(String column) {
    return _dictionaryCreatorMap.get(column);
  }

  private boolean shouldDeriveNumDocsPerChunk(String columnName, Map<String, Map<String, String>> columnProperties) {
    if (columnProperties != null) {
      Map<String, String> properties = columnProperties.get(columnName);
      return properties != null && Boolean
          .parseBoolean(properties.get(FieldConfig.DERIVE_NUM_DOCS_PER_CHUNK_RAW_INDEX_KEY));
    }
    return false;
  }

  private int rawIndexWriterVersion(String columnName, Map<String, Map<String, String>> columnProperties) {
    if (columnProperties != null && columnProperties.get(columnName) != null) {
      Map<String, String> properties = columnProperties.get(columnName);
      String version = properties.get(FieldConfig.RAW_INDEX_WRITER_VERSION);
      if (version == null) {
        return BaseChunkSVForwardIndexWriter.DEFAULT_VERSION;
      }
      return Integer.parseInt(version);
    }
    return BaseChunkSVForwardIndexWriter.DEFAULT_VERSION;
  }

  /**
   * Helper method that returns compression type to use based on segment creation spec and field type.
   * <ul>
   *   <li> Returns compression type from segment creation spec, if specified there.</li>
   *   <li> Else, returns PASS_THROUGH for metrics, and SNAPPY for dimensions. This is because metrics are likely
   *        to be spread in different chunks after applying predicates. Same could be true for dimensions, but in that
   *        case, clients are expected to explicitly specify the appropriate compression type in the spec. </li>
   * </ul>
   * @param segmentCreationSpec Segment creation spec
   * @param fieldSpec Field spec for the column
   * @return Compression type to use
   */
  private ChunkCompressorFactory.CompressionType getColumnCompressionType(SegmentGeneratorConfig segmentCreationSpec,
      FieldSpec fieldSpec) {
    ChunkCompressorFactory.CompressionType compressionType =
        segmentCreationSpec.getRawIndexCompressionType().get(fieldSpec.getName());

    if (compressionType == null) {
      if (fieldSpec.getFieldType().equals(FieldType.METRIC)) {
        return ChunkCompressorFactory.CompressionType.PASS_THROUGH;
      } else {
        return ChunkCompressorFactory.CompressionType.SNAPPY;
      }
    } else {
      return compressionType;
    }
  }

  /**
   * Returns true if dictionary should be created for a column, false otherwise.
   * Currently there are two sources for this config:
   * <ul>
   *   <li> ColumnIndexCreationInfo (this is currently hard-coded to always return dictionary). </li>
   *   <li> SegmentGeneratorConfig</li>
   * </ul>
   *
   * This method gives preference to the SegmentGeneratorConfig first.
   *
   * @param info Column index creation info
   * @param config Segment generation config
   * @param spec Field spec for the column
   * @return True if dictionary should be created for the column, false otherwise
   */
  private boolean createDictionaryForColumn(ColumnIndexCreationInfo info, SegmentGeneratorConfig config,
      FieldSpec spec) {
    String column = spec.getName();

    if (config.getRawIndexCreationColumns().contains(column) || config.getRawIndexCompressionType()
        .containsKey(column)) {
      if (!spec.isSingleValueField()) {
        throw new RuntimeException(
            "Creation of indices without dictionaries is supported for single valued columns only.");
      }
      return false;
    } else if (spec.getDataType().equals(DataType.BYTES) && !info.isFixedLength()) {
      return false;
    }
    return info.isCreateDictionary();
  }

  /**
   * Helper method to build the raw index creator for the column.
   * Assumes that column to be indexed is single valued.
   *
   * @param file Output index file
   * @param column Column name
   * @param totalDocs Total number of documents to index
   * @param lengthOfLongestEntry Length of longest entry
   * @param deriveNumDocsPerChunk true if varbyte writer should auto-derive the number of rows per chunk
   * @param writerVersion version to use for the raw index writer
   * @return raw index creator
   * @throws IOException
   */
  private ForwardIndexCreator getRawIndexCreatorForColumn(File file,
      ChunkCompressorFactory.CompressionType compressionType, String column, DataType dataType, int totalDocs,
      int lengthOfLongestEntry, boolean deriveNumDocsPerChunk, int writerVersion)
      throws IOException {
    switch (dataType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return new SingleValueFixedByteRawIndexCreator(file, compressionType, column, totalDocs, dataType,
            writerVersion);
      case STRING:
      case BYTES:
        return new SingleValueVarByteRawIndexCreator(file, compressionType, column, totalDocs, dataType,
            lengthOfLongestEntry, deriveNumDocsPerChunk, writerVersion);
      default:
        throw new UnsupportedOperationException("Data type not supported for raw indexing: " + dataType);
    }
  }

  public void index(GenericRow row) {
    for (Map.Entry<String, ForwardIndexCreator> entry : _forwardIndexCreatorMap.entrySet()) {
      String columnName = entry.getKey();
      ForwardIndexCreator forwardIndexCreator = entry.getValue();

      Object columnValueToIndex = row.getValue(columnName);
      if (columnValueToIndex == null) {
        throw new RuntimeException("Null value for column:" + columnName);
      }

      boolean isSingleValue = _schema.getFieldSpecFor(columnName).isSingleValueField();
      SegmentDictionaryCreator dictionaryCreator = _dictionaryCreatorMap.get(columnName);

      if (isSingleValue) {
        // SV column
        if (dictionaryCreator != null) {
          // dictionary encoded SV column
          // get dictID from dictionary
          int dictId = dictionaryCreator.indexOfSV(columnValueToIndex);
          // store the docID -> dictID mapping in forward index
          forwardIndexCreator.putDictId(dictId);
          DictionaryBasedInvertedIndexCreator invertedIndexCreator = _invertedIndexCreatorMap.get(columnName);
          if (invertedIndexCreator != null) {
            // if inverted index enabled during segment creation,
            // then store dictID -> docID mapping in inverted index
            invertedIndexCreator.add(dictId);
          }
        } else {
          // non-dictionary encoded SV column
          // store the docId -> raw value mapping in forward index
          switch (forwardIndexCreator.getValueType()) {
            case INT:
              forwardIndexCreator.putInt((int) columnValueToIndex);
              break;
            case LONG:
              forwardIndexCreator.putLong((long) columnValueToIndex);
              break;
            case FLOAT:
              forwardIndexCreator.putFloat((float) columnValueToIndex);
              break;
            case DOUBLE:
              forwardIndexCreator.putDouble((double) columnValueToIndex);
              break;
            case STRING:
              forwardIndexCreator.putString((String) columnValueToIndex);
              break;
            case BYTES:
              forwardIndexCreator.putBytes((byte[]) columnValueToIndex);
              break;
            default:
              throw new IllegalStateException();
          }
        }
        // text-index enabled SV column
        if (_textIndexColumns.contains(columnName)) {
          _textIndexCreatorMap.get(columnName).add((String) columnValueToIndex);
        }
      } else {
        // MV column (always dictionary encoded)
        int[] dictIds = dictionaryCreator.indexOfMV(columnValueToIndex);
        forwardIndexCreator.putDictIdMV(dictIds);
        DictionaryBasedInvertedIndexCreator invertedIndexCreator = _invertedIndexCreatorMap.get(columnName);
        if (invertedIndexCreator != null) {
          invertedIndexCreator.add(dictIds, dictIds.length);
        }
      }

      if (_nullHandlingEnabled) {
        // If row has null value for given column name, add to null value vector
        if (row.isNullValue(columnName)) {
          _nullValueVectorCreatorMap.get(columnName).setNull(_docIdCounter);
        }
      }
    }
    _docIdCounter++;
  }

  public void seal()
      throws IOException {
    for (DictionaryBasedInvertedIndexCreator invertedIndexCreator : _invertedIndexCreatorMap.values()) {
      invertedIndexCreator.seal();
    }
    for (TextIndexCreator textIndexCreator : _textIndexCreatorMap.values()) {
      textIndexCreator.seal();
    }
    for (NullValueVectorCreator nullValueVectorCreator : _nullValueVectorCreatorMap.values()) {
      nullValueVectorCreator.seal();
    }
  }

  public void close()
      throws IOException {
    FileUtils.close(Iterables
        .concat(_dictionaryCreatorMap.values(), _forwardIndexCreatorMap.values(), _invertedIndexCreatorMap.values(),
            _textIndexCreatorMap.values(), _nullValueVectorCreatorMap.values()));
  }
}
