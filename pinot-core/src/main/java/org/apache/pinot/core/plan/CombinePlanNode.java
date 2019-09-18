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
package org.apache.pinot.core.plan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.EnumUtils;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.CombineGroupByOperator;
import org.apache.pinot.core.operator.CombineGroupByOperatorV0;
import org.apache.pinot.core.operator.CombineGroupByOperatorV1;
import org.apache.pinot.core.operator.CombineGroupByOperatorV2;
import org.apache.pinot.core.operator.CombineGroupByOperatorV3;
import org.apache.pinot.core.operator.CombineOperator;
import org.apache.pinot.core.query.exception.BadQueryRequestException;
import org.apache.pinot.core.util.trace.TraceCallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.common.utils.CommonConstants.Broker.Request.*;


/**
 * The <code>CombinePlanNode</code> class provides the execution plan for combining results from multiple segments.
 */
public class CombinePlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(CombinePlanNode.class);

  /**
   * MAX_PLAN_THREADS should be >= 1.
   * Runtime.getRuntime().availableProcessors() may return value < 2 in container based environment, e.g. Kubernetes.
   */
  private static final int MAX_PLAN_THREADS =
      Math.max(1, Math.min(10, (int) (Runtime.getRuntime().availableProcessors() * .5)));
  private static final int MIN_TASKS_PER_THREAD = 10;
  private static final int TIME_OUT_IN_MILLISECONDS_FOR_PARALLEL_RUN = 10_000;

  private final List<PlanNode> _planNodes;
  private final BrokerRequest _brokerRequest;
  private final ExecutorService _executorService;
  private final long _timeOutMs;
  private final int _innerSegmentNumGroupsLimit;
  private final int _interSegmentNumGroupsLimit;

  /**
   * Constructor for the class.
   *
   * @param planNodes List of underlying plan nodes
   * @param brokerRequest Broker request
   * @param executorService Executor service
   * @param timeOutMs Time out in milliseconds for query execution (not for planning phase)
   * @param innerSegmentNumGroupsLimit Limit of number of groups stored in each segment
   * @param interSegmentNumGroupsLimit Limit of number of groups stored across all segments of the server
   */
  public CombinePlanNode(List<PlanNode> planNodes, BrokerRequest brokerRequest, ExecutorService executorService,
      long timeOutMs, int innerSegmentNumGroupsLimit, int interSegmentNumGroupsLimit) {
    _planNodes = planNodes;
    _brokerRequest = brokerRequest;
    _executorService = executorService;
    _timeOutMs = timeOutMs;
    _innerSegmentNumGroupsLimit = innerSegmentNumGroupsLimit;
    _interSegmentNumGroupsLimit = interSegmentNumGroupsLimit;
  }

  @Override
  public Operator run() {
    int numPlanNodes = _planNodes.size();
    List<Operator> operators = new ArrayList<>(numPlanNodes);

    if (numPlanNodes <= MIN_TASKS_PER_THREAD) {
      // Small number of plan nodes, run them sequentially
      for (PlanNode planNode : _planNodes) {
        operators.add(planNode.run());
      }
    } else {
      // Large number of plan nodes, run them in parallel

      // Calculate the time out timestamp
      long endTime = System.currentTimeMillis() + TIME_OUT_IN_MILLISECONDS_FOR_PARALLEL_RUN;

      int threads = Math.min(numPlanNodes / MIN_TASKS_PER_THREAD + ((numPlanNodes % MIN_TASKS_PER_THREAD == 0) ? 0 : 1),
          // ceil without using double arithmetic
          MAX_PLAN_THREADS);
      int opsPerThread = Math.max(numPlanNodes / threads + ((numPlanNodes % threads == 0) ? 0 : 1),
          // ceil without using double arithmetic
          MIN_TASKS_PER_THREAD);
      // Submit all jobs
      Future[] futures = new Future[threads];
      for (int i = 0; i < threads; i++) {
        final int index = i;
        futures[i] = _executorService.submit(new TraceCallable<List<Operator>>() {
          @Override
          public List<Operator> callJob() throws Exception {
            List<Operator> operators = new ArrayList<>();
            int start = index * opsPerThread;
            int limit = Math.min(opsPerThread, numPlanNodes - start);
            for (int count = start; count < start + limit; count++) {
              operators.add(_planNodes.get(count).run());
            }
            return operators;
          }
        });
      }

      // Get all results
      try {
        for (Future future : futures) {
          List<Operator> ops = (List<Operator>) future.get(endTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
          operators.addAll(ops);
        }
      } catch (Exception e) {
        // Future object will throw ExecutionException for execution exception, need to check the cause to determine
        // whether it is caused by bad query
        Throwable cause = e.getCause();
        if (cause instanceof BadQueryRequestException) {
          throw (BadQueryRequestException) cause;
        } else {
          throw new RuntimeException("Caught exception while running CombinePlanNode.", e);
        }
      } finally {
        // Cancel all ongoing jobs
        for (Future future : futures) {
          if (!future.isDone()) {
            future.cancel(true);
          }
        }
      }
    }

    // TODO: use the same combine operator for both aggregation and selection query.
    if (_brokerRequest.isSetAggregationsInfo() && _brokerRequest.getGroupBy() != null) {
      // Aggregation group-by query
      Map<String, String> queryOptions = _brokerRequest.getQueryOptions();
      GroupByMode groupByMode = GroupByMode.PQL;
      if (queryOptions != null) {
        String groupByModeValue = queryOptions.get(QueryOptionKey.GROUP_BY_MODE);
        if (groupByModeValue != null && EnumUtils.isValidEnum(GroupByMode.class, groupByModeValue.toUpperCase())) {
          groupByMode = GroupByMode.valueOf(groupByModeValue.toUpperCase());
        }
      }
      switch (groupByMode) {

        case V0:
          // V0 - AggregationGroupByOperator untouched. Concurrent inside CombineGroupByOperator
          return new CombineGroupByOperatorV0(operators, _brokerRequest, _executorService, _timeOutMs,
              _interSegmentNumGroupsLimit);
        case V1:
          // V1 - Simple inside AggregationGroupByOperator, Concurrent inside CombineGroupByOperator
          return new CombineGroupByOperatorV1(operators, _brokerRequest, _executorService, _timeOutMs,
              _interSegmentNumGroupsLimit);
        case V2:
          // V2 - Simple in AggregationGroupByOperator, Simple in CombineGroupByOperator
          return new CombineGroupByOperatorV2(operators, _brokerRequest, _interSegmentNumGroupsLimit);
        case V3:
          // V3 - One Concurrent across everything
          return new CombineGroupByOperatorV3(operators, _brokerRequest, _executorService, _timeOutMs,
              _interSegmentNumGroupsLimit);
        case V4:
          // V4 - One Concurrent across everything, but inside AggregationGroupByOperator, use Simple/existing approach, then copy over
        case V5:
          // V5 - One Concurrent across everything, use local dictionary
        case V6:
          // other special cases?
        case PQL:
        default:
          return new CombineGroupByOperator(operators, _brokerRequest, _executorService, _timeOutMs,
              _innerSegmentNumGroupsLimit, _interSegmentNumGroupsLimit);

      }
    } else {
      // Selection or aggregation only query
      return new CombineOperator(operators, _executorService, _timeOutMs, _brokerRequest);
    }
  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug(prefix + "Instance Level Inter-Segments Combine Plan Node:");
    LOGGER.debug(prefix + "Operator: CombineOperator/CombineGroupByOperator");
    LOGGER.debug(prefix + "Argument 0: BrokerRequest - " + _brokerRequest);
    int i = 1;
    for (PlanNode planNode : _planNodes) {
      LOGGER.debug(prefix + "Argument " + (i++) + ":");
      planNode.showTree(prefix + "    ");
    }
  }
}
