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
package org.apache.pinot.core.operator.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.GroupBy;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.data.table.SimpleIndexedTable;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.query.aggregation.AggregationFunctionContext;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;


/**
 * The <code>AggregationOperator</code> class provides the operator for aggregation group-by query on a single segment.
 */
// Each operator has its own Table
public class AggregationGroupByOperatorV1 extends BaseAggregationGroupByOperator {
  private static final String OPERATOR_NAME = AggregationGroupByOperatorV1.class.getSimpleName();

  private final DataSchema _dataSchema;

  private final List<AggregationInfo> _aggregationInfos;
  private final AggregationFunctionContext[] _functionContexts;
  private final List<SelectionSort> _orderBy;
  private final TransformExpressionTree[] _groupByExpressions;
  private final boolean[] _isSingleValue;
  private final boolean _hasMV;
  private final TransformExpressionTree[] _aggregationExpressions;
  private final int _innerSegmentNumGroupsLimit;
  private final int _numColumns;
  private final int _numGroupBy;
  private final int _numAggregations;
  private final String[] _columnNames;
  private final DataSchema.ColumnDataType[] _columnDataTypes;
  private final TransformOperator _transformOperator;
  private final long _numTotalRawDocs;
  private final boolean _useStarTree;

  // FIXME: using star tree

  // FIXME: BYTES support end to end

  private ExecutionStatistics _executionStatistics;

  public AggregationGroupByOperatorV1(@Nonnull List<AggregationInfo> aggregationInfos,
      @Nonnull AggregationFunctionContext[] functionContexts, @Nonnull GroupBy groupBy, List<SelectionSort> orderBy,
      int innerSegmentNumGroupsLimit, int interSegmentNumGroupsLimit, @Nonnull TransformOperator transformOperator,
      long numTotalRawDocs, boolean useStarTree) {
    _aggregationInfos = aggregationInfos;
    _functionContexts = functionContexts;
    _orderBy = orderBy;
    _innerSegmentNumGroupsLimit = innerSegmentNumGroupsLimit;
    _transformOperator = transformOperator;
    _numTotalRawDocs = numTotalRawDocs;
    _useStarTree = useStarTree;

    _numGroupBy = groupBy.getExpressionsSize();
    _numAggregations = _functionContexts.length;
    _numColumns = _numAggregations + _numGroupBy;
    _groupByExpressions = new TransformExpressionTree[_numGroupBy];
    _isSingleValue = new boolean[_numGroupBy];
    _aggregationExpressions = new TransformExpressionTree[_numAggregations];
    _columnNames = new String[_numColumns];
    _columnDataTypes = new DataSchema.ColumnDataType[_numColumns];

    Map<String, DataSchema.ColumnDataType> columnToDataType = new HashMap<>(_numColumns);
    Map<String, Boolean> columnToIsSingleValue = new HashMap<>(_numColumns);
    boolean hasMV = false;
    for (TransformExpressionTree transformExpression : _transformOperator.getExpressions()) {
      TransformResultMetadata resultMetadata = _transformOperator.getResultMetadata(transformExpression);
      String transformExpressionString = transformExpression.toString();
      columnToDataType.put(transformExpressionString,
          DataSchema.ColumnDataType.fromDataType(resultMetadata.getDataType(), true));
      columnToIsSingleValue.put(transformExpressionString, resultMetadata.isSingleValue());
      hasMV |= !resultMetadata.isSingleValue();
    }
    _hasMV = hasMV;

    int index = 0;

    // extract column names and data types for group by keys
    List<String> expressions = groupBy.getExpressions();
    for (int i = 0; i < _numGroupBy; i++) {
      String groupByExpression = expressions.get(i);
      _groupByExpressions[i] = TransformExpressionTree.compileToExpressionTree(groupByExpression);
      _columnNames[index] = groupByExpression;
      _columnDataTypes[index] = columnToDataType.get(groupByExpression);
      _isSingleValue[i] = columnToIsSingleValue.get(groupByExpression);
      index++;
    }

    // extract column names and data types for aggregations
    for (int i = 0; i < _numAggregations; i++) {
      AggregationFunctionContext functionContext = functionContexts[i];
      if (functionContext.getAggregationFunction().getType() != AggregationFunctionType.COUNT) {
        _aggregationExpressions[i] = TransformExpressionTree.compileToExpressionTree(functionContext.getColumn());
      }
      _columnNames[index] = functionContext.getAggregationFunction().getType().toString().toLowerCase() + "("
          + functionContext.getColumn() + ")";
      _columnDataTypes[index] = functionContext.getAggregationFunction().getIntermediateResultColumnType();
      index++;
    }

    _dataSchema = new DataSchema(_columnNames, _columnDataTypes);
  }

  @Override
  protected IntermediateResultsBlock getNextBlock() {
    IndexedTable indexedTable = new SimpleIndexedTable();
    // initialize with capacity innerSegmentNumGroupsLimit
    indexedTable.init(_dataSchema, _aggregationInfos, _orderBy, _innerSegmentNumGroupsLimit, false);

    int numDocsScanned = 0;

    TransformBlock transformBlock;
    while ((transformBlock = _transformOperator.nextBlock()) != null) {
      int numDocs = transformBlock.getNumDocs();
      numDocsScanned += numDocs;

      Object[][] valuesList = new Object[_numColumns][];

      int index = 0;
      for (int i = 0; i < _numGroupBy; i++) {
        BlockValSet blockValueSet = transformBlock.getBlockValueSet(_groupByExpressions[i]);
        DataSchema.ColumnDataType columnDataType = _columnDataTypes[index];
        if (_isSingleValue[i]) {
          valuesList[index] = getValuesSV(blockValueSet, numDocs, columnDataType);
        } else {
          valuesList[index] = getValuesMV(blockValueSet, numDocs, columnDataType);
        }
        index++;
      }

      for (int i = 0; i < _numAggregations; i++) {
        AggregationFunction aggregationFunction = _functionContexts[i].getAggregationFunction();
        if (aggregationFunction.getType() == AggregationFunctionType.COUNT) {
          valuesList[index] = aggregationFunction.getValuesFromBlock(null, numDocs);
        } else {
          BlockValSet blockValueSet = transformBlock.getBlockValueSet(_aggregationExpressions[i]);
          valuesList[index] = aggregationFunction.getValuesFromBlock(blockValueSet, numDocs);
        }
        index++;
      }

      for (int docId = 0; docId < numDocs; docId++) {
        Object[] groupByValues = new Object[_numGroupBy];
        Object[] aggregationValues = new Object[_numAggregations];

        if (_hasMV) {
          index = 0;
          List<Object[]> sets = new ArrayList<>(_numGroupBy);
          for (int groupByIndex = 0; groupByIndex < _numGroupBy; groupByIndex++) {
            Object value = valuesList[index][docId];
            Object[] set;
            if (_isSingleValue[index]) {
              set = new Object[]{value};
            } else {
              set = (Object[]) value;
            }
            sets.add(set);
            index++;
          }
          for (int aggregationIndex = 0; aggregationIndex < _numAggregations; aggregationIndex++) {
            aggregationValues[aggregationIndex] = valuesList[index++][docId];
          }

          List<Object[]> cartesianProduct = cartesianProduct(sets);
          for (Object[] combination : cartesianProduct) {
            Record record = new Record(new Key(combination), aggregationValues.clone());
            indexedTable.upsert(record);
          }
        } else {
          index = 0;
          for (int groupByIndex = 0; groupByIndex < _numGroupBy; groupByIndex++) {
            groupByValues[groupByIndex] = valuesList[index++][docId];
          }
          for (int aggregationIndex = 0; aggregationIndex < _numAggregations; aggregationIndex++) {
            aggregationValues[aggregationIndex] = valuesList[index++][docId];
          }
          Record record = new Record(new Key(groupByValues), aggregationValues);
          indexedTable.upsert(record);
        }
      }
    }


    // Gather execution statistics
    long numEntriesScannedInFilter = _transformOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    long numEntriesScannedPostFilter = numDocsScanned * _transformOperator.getNumColumnsProjected();
    _executionStatistics =
        new ExecutionStatistics(numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter,
            _numTotalRawDocs);

    // send Indexed Table as results
    return new IntermediateResultsBlock(indexedTable);
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return _executionStatistics;
  }
}
