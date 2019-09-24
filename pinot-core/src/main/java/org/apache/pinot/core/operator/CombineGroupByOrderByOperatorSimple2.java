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
package org.apache.pinot.core.operator;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.BytesUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.ConcurrentIndexedTable;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.data.table.SimpleIndexedTable;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.util.GroupByUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>CombineGroupByOrderByOperator</code> class is the operator to combine aggregation results with group-by and order by.
 */
// TODO: this class has a lot of duplication with {@link CombineGroupByOperator}.
// These 2 classes can be combined into one
// For the first iteration of Order By support, these will be separate
public class CombineGroupByOrderByOperatorSimple2 extends BaseOperator<IntermediateResultsBlock> {
  private static final Logger LOGGER = LoggerFactory.getLogger(CombineGroupByOrderByOperatorSimple2.class);
  private static final String OPERATOR_NAME = "CombineGroupByOrderByOperator2";

  private final List<Operator> _operators;
  private final BrokerRequest _brokerRequest;
  private final int _interSegmentNumGroupsLimit;
  private DataSchema _dataSchema;
  private ConcurrentIndexedTable _indexedTable;

  public CombineGroupByOrderByOperatorSimple2(List<Operator> operators, BrokerRequest brokerRequest) {
    Preconditions.checkArgument(brokerRequest.isSetAggregationsInfo() && brokerRequest.isSetGroupBy());

    _operators = operators;
    _brokerRequest = brokerRequest;
    _indexedTable = new ConcurrentIndexedTable();
    _interSegmentNumGroupsLimit = GroupByUtils.getTableCapacity((int) brokerRequest.getGroupBy().getTopN());
  }

  /**
   * {@inheritDoc}
   *
   * <p>Combines the group-by result blocks from underlying operators and returns a merged and trimmed group-by
   * result block.
   * <ul>
   *   <li>
   *     Sequentially merge group-by results from multiple result blocks into {@link org.apache.pinot.core.data.table.IndexedTable}
   *   </li>
   *   <li>
   *     Set all exceptions encountered during execution into the merged result block
   *   </li>
   * </ul>
   */
  @Override
  protected IntermediateResultsBlock getNextBlock() {
    int numOperators = _operators.size();

    int numAggregationFunctions = _brokerRequest.getAggregationsInfoSize();
    int numGroupBy = _brokerRequest.getGroupBy().getExpressionsSize();
    List<ProcessingException> mergedProcessingExceptions = new ArrayList<>();

    for (int i = 0; i < numOperators; i++) {
      AggregationGroupByResult aggregationGroupByResult;

      try {
        IntermediateResultsBlock intermediateResultsBlock = (IntermediateResultsBlock) _operators.get(i).nextBlock();

        if (_dataSchema == null) {
          _dataSchema = intermediateResultsBlock.getDataSchema();
          _indexedTable.init(_dataSchema, _brokerRequest.getAggregationsInfo(), _brokerRequest.getOrderBy(),
              _interSegmentNumGroupsLimit, false);
        }

        // Merge processing exceptions.
        List<ProcessingException> processingExceptionsToMerge = intermediateResultsBlock.getProcessingExceptions();
        if (processingExceptionsToMerge != null) {
          mergedProcessingExceptions.addAll(processingExceptionsToMerge);
        }

        // Merge aggregation group-by result.
        aggregationGroupByResult = intermediateResultsBlock.getAggregationGroupByResult();
        if (aggregationGroupByResult != null) {
          // Get converter functions
          Function[] converterFunctions = new Function[numGroupBy];
          for (int j = 0; j < numGroupBy; j++) {
            converterFunctions[j] = getConverterFunction(_dataSchema.getColumnDataType(j));
          }

          // Iterate over the group-by keys, for each key, update the group-by result in the indexedTable.
          Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = aggregationGroupByResult.getGroupKeyIterator();
          while (groupKeyIterator.hasNext()) {
            GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
            String[] stringKey = groupKey._stringKey.split(GroupKeyGenerator.DELIMITER);
            Object[] objectKey = new Object[numGroupBy];
            for (int j = 0; j < stringKey.length; j++) {
              objectKey[j] = converterFunctions[j].apply(stringKey[j]);
            }
            Object[] values = new Object[numAggregationFunctions];
            for (int j = 0; j < numAggregationFunctions; j++) {
              values[j] = aggregationGroupByResult.getResultForKey(groupKey, j);
            }

            Record record = new Record(new Key(objectKey), values);
            _indexedTable.upsert(record);
          }
        }
      } catch (Exception e) {
        LOGGER.error("Exception processing CombineGroupByOrderBy for index {}, operator {}", i,
            _operators.get(i).getClass().getName(), e);
        mergedProcessingExceptions.add(QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, e));
      }
    }

    try {
      _indexedTable.finish();
      IntermediateResultsBlock mergedBlock = new IntermediateResultsBlock(_indexedTable);

      // Set the processing exceptions.
      if (!mergedProcessingExceptions.isEmpty()) {
        mergedBlock.setProcessingExceptions(mergedProcessingExceptions);
      }

      // Set the execution statistics.
      ExecutionStatistics executionStatistics = new ExecutionStatistics();
      for (Operator operator : _operators) {
        ExecutionStatistics executionStatisticsToMerge = operator.getExecutionStatistics();
        if (executionStatisticsToMerge != null) {
          executionStatistics.merge(executionStatisticsToMerge);
        }
      }
      mergedBlock.setNumDocsScanned(executionStatistics.getNumDocsScanned());
      mergedBlock.setNumEntriesScannedInFilter(executionStatistics.getNumEntriesScannedInFilter());
      mergedBlock.setNumEntriesScannedPostFilter(executionStatistics.getNumEntriesScannedPostFilter());
      mergedBlock.setNumSegmentsProcessed(executionStatistics.getNumSegmentsProcessed());
      mergedBlock.setNumSegmentsMatched(executionStatistics.getNumSegmentsMatched());
      mergedBlock.setNumTotalRawDocs(executionStatistics.getNumTotalRawDocs());

      if (_indexedTable.size() >= _interSegmentNumGroupsLimit) {
        mergedBlock.setNumGroupsLimitReached(true);
      }

      return mergedBlock;
    } catch (Exception e) {
      return new IntermediateResultsBlock(e);
    }
  }

  private Function<String, Object> getConverterFunction(DataSchema.ColumnDataType columnDataType) {
    Function<String, Object> function;
    switch (columnDataType) {

      case INT:
        function = Integer::valueOf;
        break;
      case LONG:
        function = Long::valueOf;
        break;
      case FLOAT:
        function = Float::valueOf;
        break;
      case DOUBLE:
        function = Double::valueOf;
        break;
      case BYTES:
        function = BytesUtils::toBytes;
        break;
      case STRING:
      default:
        function = s -> s;
        break;
    }
    return function;
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
