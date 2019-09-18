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
package org.apache.pinot.core.query.aggregation.function;

import javax.annotation.Nonnull;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;


public class DistinctCountRawHLLMVAggregationFunction extends DistinctCountRawHLLAggregationFunction {

  private final DistinctCountHLLMVAggregationFunction _distinctCountHLLMVAggregationFunction = new DistinctCountHLLMVAggregationFunction();

  @Nonnull
  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTRAWHLLMV;
  }

  @Nonnull
  @Override
  public String getColumnName(@Nonnull String column) {
    return AggregationFunctionType.DISTINCTCOUNTRAWHLLMV.getName() + "_" + column;
  }

  @Override
  public void aggregate(int length, @Nonnull AggregationResultHolder aggregationResultHolder,
      @Nonnull BlockValSet... blockValSets) {
    _distinctCountHLLMVAggregationFunction.aggregate(length, aggregationResultHolder, blockValSets);
  }

  @Override
  public void aggregateGroupBySV(int length, @Nonnull int[] groupKeyArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    _distinctCountHLLMVAggregationFunction.aggregateGroupBySV(length, groupKeyArray, groupByResultHolder, blockValSets);
  }

  @Override
  public void aggregateGroupByMV(int length, @Nonnull int[][] groupKeysArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    _distinctCountHLLMVAggregationFunction.aggregateGroupByMV(length, groupKeysArray, groupByResultHolder, blockValSets);
  }

  @Override
  public Object[] getValuesFromBlock(BlockValSet blockValueSet, int numDocs) {
    return _distinctCountHLLMVAggregationFunction.getValuesFromBlock(blockValueSet, numDocs);
  }
}
