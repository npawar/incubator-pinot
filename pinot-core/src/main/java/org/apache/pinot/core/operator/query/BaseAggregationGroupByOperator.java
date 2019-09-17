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
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;


public abstract class BaseAggregationGroupByOperator extends BaseOperator<IntermediateResultsBlock> {

  Object[] getCountValues(int numDocs) {
    Object[] values = new Object[numDocs];
    Arrays.fill(values, 1L);
    return values;
  }

  Object[] getValuesSV(BlockValSet blockValueSet, int numDocs, DataSchema.ColumnDataType columnDataType) {
    Object[] values = new Object[numDocs];
    switch (columnDataType) {

      case INT:
        int[] intValues = blockValueSet.getIntValuesSV();
        for (int i = 0; i < numDocs; i++) {
          values[i] = intValues[i];
        }
        break;
      case LONG:
        long[] longValues = blockValueSet.getLongValuesSV();
        for (int i = 0; i < numDocs; i++) {
          values[i] = longValues[i];
        }
        break;
      case FLOAT:
        float[] floatValues = blockValueSet.getFloatValuesSV();
        for (int i = 0; i < numDocs; i++) {
          values[i] = floatValues[i];
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValueSet.getDoubleValuesSV();
        for (int i = 0; i < numDocs; i++) {
          values[i] = doubleValues[i];
        }
        break;
      case STRING:
        String[] stringValues = blockValueSet.getStringValuesSV();
        for (int i = 0; i < numDocs; i++) {
          values[i] = stringValues[i];
        }
        break;
      case BYTES:
        byte[][] bytesValues = blockValueSet.getBytesValuesSV();
        for (int i = 0; i < numDocs; i++) {
          values[i] = bytesValues[i];
        }
        break;
    }
    return values;
  }

  Object[] getValuesMV(BlockValSet blockValueSet, int numDocs, DataSchema.ColumnDataType columnDataType) {
    Object[] values = new Object[numDocs];
    switch (columnDataType) {

      case INT:
        int[][] intValues = blockValueSet.getIntValuesMV();
        for (int i = 0; i < numDocs; i++) {
          int[] intValue = intValues[i];
          Object[] objectArray = new Object[intValue.length];
          for (int j = 0; j < intValue.length; j++) {
            objectArray[j] = intValue[j];
          }
          values[i] = objectArray;
        }
        break;
      case LONG:
        long[][] longValues = blockValueSet.getLongValuesMV();
        for (int i = 0; i < numDocs; i++) {
          long[] longValue = longValues[i];
          Object[] objectArray = new Object[longValue.length];
          for (int j = 0; j < longValue.length; j++) {
            objectArray[j] = longValue[j];
          }
          values[i] = objectArray;
        }
        break;
      case FLOAT:
        float[][] floatValues = blockValueSet.getFloatValuesMV();
        for (int i = 0; i < numDocs; i++) {
          float[] floatValue = floatValues[i];
          Object[] objectArray = new Object[floatValue.length];
          for (int j = 0; j < floatValue.length; j++) {
            objectArray[j] = floatValue[j];
          }
          values[i] = objectArray;
        }
        break;
      case DOUBLE:
        double[][] doubleValues = blockValueSet.getDoubleValuesMV();
        for (int i = 0; i < numDocs; i++) {
          double[] doubleValue = doubleValues[i];
          Object[] objectArray = new Object[doubleValue.length];
          for (int j = 0; j < doubleValue.length; j++) {
            objectArray[j] = doubleValue[j];
          }
          values[i] = objectArray;
        }
        break;
      case STRING:
        String[][] stringValues = blockValueSet.getStringValuesMV();
        for (int i = 0; i < numDocs; i++) {
          String[] stringValue = stringValues[i];
          Object[] objectArray = new Object[stringValue.length];
          for (int j = 0; j < stringValue.length; j++) {
            objectArray[j] = stringValue[j];
          }
          values[i] = objectArray;
        }
        break;
    }
    return values;
  }

  List<Object[]> cartesianProduct(List<Object[]> lists) {
    int numElements = 1;
    for (int i = 0; i < lists.size(); i++) {
      numElements *= lists.get(i).length;
    }

    List<Object[]> result = new ArrayList<>();
    for (int i = 0; i < numElements; i++) {
      int j = 1;

      Object[] oneSet = new Object[lists.size()];
      int oneSetIdx = 0;
      for (Object[] list : lists) {
        int index = (i / j) % list.length;
        oneSet[oneSetIdx++] = list[index];
        j *= list.length;
      }
      result.add(oneSet);
    }
    return result;
  }

}
