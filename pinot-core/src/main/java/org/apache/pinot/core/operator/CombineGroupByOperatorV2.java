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
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.ConcurrentIndexedTable;
import org.apache.pinot.core.data.table.SimpleIndexedTable;
import org.apache.pinot.core.data.table.Table;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.util.trace.TraceRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>CombineGroupByOperatorV2</code> class is the operator to combine aggregation group-by results.
 * It uses a {@link SimpleIndexedTable} to merge the results across all {@link Table} received from each segment level operator
 */
public class CombineGroupByOperatorV2 extends BaseOperator<IntermediateResultsBlock> {
  private static final Logger LOGGER = LoggerFactory.getLogger(CombineGroupByOperatorV2.class);
  private static final String OPERATOR_NAME = CombineGroupByOperatorV2.class.getSimpleName();

  private final List<Operator> _operators;
  private final BrokerRequest _brokerRequest;
  private final int _interSegmentNumGroupsLimit;
  private DataSchema _dataSchema;

  private SimpleIndexedTable _indexedTable;

  public CombineGroupByOperatorV2(List<Operator> operators, BrokerRequest brokerRequest,
      int interSegmentNumGroupsLimit) {
    Preconditions.checkArgument(brokerRequest.isSetAggregationsInfo() && brokerRequest.isSetGroupBy());

    _operators = operators;
    _brokerRequest = brokerRequest;
    _interSegmentNumGroupsLimit = interSegmentNumGroupsLimit;
    _indexedTable = new SimpleIndexedTable();
  }

  /**
   * Merge results across all segments using a {@link SimpleIndexedTable} with capacity _interSegmentNumGroupsLimit
   */
  @Override
  protected IntermediateResultsBlock getNextBlock() {
    List<ProcessingException> mergedProcessingExceptions = new ArrayList<>();

    for (Operator operator : _operators) {

      try {
        IntermediateResultsBlock intermediateResultsBlock = (IntermediateResultsBlock) operator.nextBlock();

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

        // Merge indexed table
        Table table = intermediateResultsBlock.getTable();
        _indexedTable.merge(table);
      } catch (Exception e) {
        LOGGER.error("Exception processing CombineGroupBy for operator {}", operator.getClass().getName(), e);
        mergedProcessingExceptions.add(QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, e));
      }
    }

    try {
      _indexedTable.finish();
      IntermediateResultsBlock mergedBlock = new IntermediateResultsBlock(_indexedTable);

      // Set the processing exceptions.
      if (!mergedProcessingExceptions.isEmpty()) {
        mergedBlock.setProcessingExceptions(new ArrayList<>(mergedProcessingExceptions));
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

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
