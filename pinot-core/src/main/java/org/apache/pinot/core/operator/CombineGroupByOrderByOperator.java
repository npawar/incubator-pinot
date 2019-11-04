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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.BytesUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.ConcurrentIndexedTable;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.data.table.SimpleIndexedTable;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.reduce.CombineService;
import org.apache.pinot.core.util.GroupByUtils;
import org.apache.pinot.core.util.trace.TraceCallable;
import org.apache.pinot.core.util.trace.TraceRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>CombineGroupByOrderByOperator</code> class is the operator to combine aggregation results with group-by and order by.
 */
// TODO: this class has a lot of duplication with {@link CombineGroupByOperator}.
// These 2 classes can be combined into one
// For the first iteration of Order By support, these will be separate
public class CombineGroupByOrderByOperator extends BaseOperator<IntermediateResultsBlock> {
  private static final Logger LOGGER = LoggerFactory.getLogger(CombineGroupByOrderByOperator.class);
  private static final String OPERATOR_NAME = "CombineGroupByOrderByOperator";

  private final List<Operator> _operators;
  private final BrokerRequest _brokerRequest;
  private final ExecutorService _executorService;
  private final long _timeOutMs;
  private final int _indexedTableCapacity;
  private DataSchema _dataSchema;
  private SimpleIndexedTable _indexedTable;

  private static final int MIN_THREADS_PER_QUERY;
  private static final int MAX_THREADS_PER_QUERY;
  private static final int MIN_SEGMENTS_PER_THREAD = 10;

  static {
    int numCores = Runtime.getRuntime().availableProcessors();
    MIN_THREADS_PER_QUERY = Math.max(1, (int) (numCores * .5));
    //Dont have more than 10 threads per query
    MAX_THREADS_PER_QUERY = Math.min(10, (int) (numCores * .5));
  }

  public CombineGroupByOrderByOperator(List<Operator> operators, BrokerRequest brokerRequest,
      ExecutorService executorService, long timeOutMs) {
    Preconditions.checkArgument(brokerRequest.isSetAggregationsInfo() && brokerRequest.isSetGroupBy());

    _operators = operators;
    _executorService = executorService;
    _brokerRequest = brokerRequest;
    _timeOutMs = timeOutMs;
    _indexedTableCapacity = GroupByUtils.getTableCapacity(brokerRequest.getGroupBy(), brokerRequest.getOrderBy());
  }

  /**
   * {@inheritDoc}
   *
   * <p>Combines the group-by result blocks from underlying operators and returns a merged and trimmed group-by
   * result block.
   * <ul>
   *   <li>
   *     Concurrently merge group-by results from multiple result blocks into {@link org.apache.pinot.core.data.table.IndexedTable}
   *   </li>
   *   <li>
   *     Set all exceptions encountered during execution into the merged result block
   *   </li>
   * </ul>
   */
  @Override
  protected IntermediateResultsBlock getNextBlock() {

    final long startTime = System.currentTimeMillis();
    final long queryEndTime = System.currentTimeMillis() + _timeOutMs;
    final int numOperators = _operators.size();
    final int numGroups = Math.min(numOperators, Math.max(MIN_THREADS_PER_QUERY,
        Math.min(MAX_THREADS_PER_QUERY, (numOperators + MIN_SEGMENTS_PER_THREAD - 1) / MIN_SEGMENTS_PER_THREAD)));

    final List<List<Operator>> operatorGroups = new ArrayList<>(numGroups);
    for (int i = 0; i < numGroups; i++) {
      operatorGroups.add(new ArrayList<>());
    }
    for (int i = 0; i < numOperators; i++) {
      operatorGroups.get(i % numGroups).add(_operators.get(i));
    }


    final int numAggregationFunctions = _brokerRequest.getAggregationsInfoSize();
    final int numGroupBy = _brokerRequest.getGroupBy().getExpressionsSize();
    final BlockingQueue<SimpleIndexedTable> blockingQueue = new ArrayBlockingQueue<>(numGroups);
    final ConcurrentLinkedQueue<ProcessingException> mergedProcessingExceptions = new ConcurrentLinkedQueue<>();

    // Submit operators.
    for (final List<Operator> operatorGroup : operatorGroups) {
      _executorService.submit(new TraceRunnable() {
        @Override
        public void runJob() {
          SimpleIndexedTable innerSimpleTable = null;
          DataSchema dataSchema = null;
          Function[] converterFunctions = null;
          try {
            for (Operator operator : operatorGroup) {
              IntermediateResultsBlock blockToMerge = (IntermediateResultsBlock) operator.nextBlock();
              if (dataSchema == null) {
                dataSchema = blockToMerge.getDataSchema();
                converterFunctions = getConverterFunctionsArray(dataSchema, numGroupBy);
                innerSimpleTable = new SimpleIndexedTable(dataSchema, _brokerRequest.getAggregationsInfo(),
                    _brokerRequest.getOrderBy(), _indexedTableCapacity);
              }
              try {
                processBlock(blockToMerge, innerSimpleTable, numGroupBy, numAggregationFunctions, converterFunctions,
                    mergedProcessingExceptions);
              } catch (Exception e) {
                LOGGER.error("Caught exception while merging two blocks (step 1).", e);
                mergedProcessingExceptions.add(QueryException.getException(QueryException.MERGE_RESPONSE_ERROR, e));
              }
            }
          } catch (Exception e) {
            LOGGER.error("Caught exception while executing query.", e);
            mergedProcessingExceptions.add(QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, e));
          }
          assert innerSimpleTable != null;
          innerSimpleTable.finish(false);
          blockingQueue.offer(innerSimpleTable);
        }
      });
    }
    LOGGER.debug("Submitting operators to be run in parallel and it took:" + (System.currentTimeMillis() - startTime));

    // Submit merger job:
    Future<SimpleIndexedTable> simpleIndexedTableFuture =
        _executorService.submit(new TraceCallable<SimpleIndexedTable>() {
          @Override
          public SimpleIndexedTable callJob() {
            int mergedBlocksNumber = 0;
            SimpleIndexedTable simpleIndexedTable = null;
            while (mergedBlocksNumber < numGroups) {
              try {
                SimpleIndexedTable tableToMerge =
                    blockingQueue.poll(queryEndTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                if (tableToMerge != null) {
                  if (simpleIndexedTable == null) {
                    simpleIndexedTable =
                        new SimpleIndexedTable(tableToMerge.getDataSchema(), _brokerRequest.getAggregationsInfo(),
                            _brokerRequest.getOrderBy(), _indexedTableCapacity);
                  }
                  simpleIndexedTable.merge(tableToMerge);
                }
              } catch (Exception e) {
                mergedProcessingExceptions.add(QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, e));
              }
              mergedBlocksNumber++;
            }
            return simpleIndexedTable;
          }
        });

    // Get merge results.
    try {
      _indexedTable = simpleIndexedTableFuture.get(queryEndTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOGGER.error("Caught InterruptedException.", e);
      mergedProcessingExceptions.add(QueryException.getException(QueryException.FUTURE_CALL_ERROR, e));
    } catch (ExecutionException e) {
      LOGGER.error("Caught ExecutionException.", e);
      mergedProcessingExceptions.add(QueryException.getException(QueryException.MERGE_RESPONSE_ERROR, e));
    } catch (TimeoutException e) {
      LOGGER.error("Caught TimeoutException", e);
      simpleIndexedTableFuture.cancel(true);
      mergedProcessingExceptions.add(QueryException.getException(QueryException.EXECUTION_TIMEOUT_ERROR, e));
    }

    try {
      _indexedTable.finish(false);
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
        function = BytesUtils::toByteArray;
        break;
      case STRING:
      default:
        function = s -> s;
        break;
    }
    return function;
  }

  private void processBlock(IntermediateResultsBlock intermediateResultsBlock, SimpleIndexedTable simpleIndexedTable,
      int numGroupBy, int numAggregationFunctions, Function[] converterFunctions,
      ConcurrentLinkedQueue<ProcessingException> mergedProcessingExceptions) {

    // Merge processing exceptions.
    List<ProcessingException> processingExceptionsToMerge = intermediateResultsBlock.getProcessingExceptions();
    if (processingExceptionsToMerge != null) {
      mergedProcessingExceptions.addAll(processingExceptionsToMerge);
    }

    // Merge aggregation group-by result.
    AggregationGroupByResult aggregationGroupByResult = intermediateResultsBlock.getAggregationGroupByResult();
    if (aggregationGroupByResult != null) {

      // Iterate over the group-by keys, for each key, update the group-by result in the indexedTable.
      Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = aggregationGroupByResult.getGroupKeyIterator();
      while (groupKeyIterator.hasNext()) {
        GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
        String[] stringKey = groupKey._stringKey.split(GroupKeyGenerator.DELIMITER);
        Object[] objectKey = new Object[numGroupBy];
        for (int i = 0; i < stringKey.length; i++) {
          objectKey[i] = converterFunctions[i].apply(stringKey[i]);
        }
        Object[] values = new Object[numAggregationFunctions];
        for (int i = 0; i < numAggregationFunctions; i++) {
          values[i] = aggregationGroupByResult.getResultForKey(groupKey, i);
        }

        Record record = new Record(new Key(objectKey), values);
        simpleIndexedTable.upsert(record);
      }
    }
  }

  private Function[] getConverterFunctionsArray(DataSchema dataSchema, int numGroupBy) {
    // Get converter functions
    Function[] converterFunctions = new Function[numGroupBy];
    for (int i = 0; i < numGroupBy; i++) {
      converterFunctions[i] = getConverterFunction(dataSchema.getColumnDataType(i));
    }
    return converterFunctions;
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
