/*
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

package org.apache.spark.sql.vectorized;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils;
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

import java.util.function.UnaryOperator;

public class ColumnarBatchUtils {

  /**
   * Create a {@link ColumnVector} of {@code numRows} rows where every value is null.
   * Uses {@link ConstantColumnVector} for memory efficiency.
   */
  public static ColumnVector createNullVector(int numRows, DataType dataType) {
    ConstantColumnVector vec = new ConstantColumnVector(numRows, dataType);
    vec.setNull();
    return vec;
  }

  /**
   * Create a constant {@link ColumnVector} of {@code numRows} rows, all set to the value at
   * {@code fieldIdx} in {@code partitionValues}.  Uses {@link ColumnVectorUtils#populate}
   * which requires {@link ConstantColumnVector} as of Spark 3.4+.
   */
  public static ColumnVector createPartitionVector(int numRows, DataType dataType,
                                                   InternalRow partitionValues, int fieldIdx) {
    ConstantColumnVector vec = new ConstantColumnVector(numRows, dataType);
    ColumnVectorUtils.populate(vec, partitionValues, fieldIdx);
    return vec;
  }

  public static UnaryOperator<ColumnarBatch> generateProjection(StructType from, StructType to) {
    if (from.length() < to.length()) {
      throw new IllegalStateException(from + " has less columns than " + to);
    }

    if (from.equals(to)) {
      return UnaryOperator.identity();
    }

    int[] projection = new int[to.size()];
    for (int i = 0; i < to.length(); i++) {
      projection[i] = from.fieldIndex(to.fields()[i].name());
    }

    return columnarBatch -> {
      ColumnVector[] vectors = new ColumnVector[projection.length];
      for (int i = 0; i < projection.length; i++) {
        vectors[i] = columnarBatch.column(projection[i]);
      }

      //TODO: [HUDI-8099] replace this with inplace projection by extending columnar batch
      ColumnarBatch b = new ColumnarBatch(vectors);
      b.setNumRows(columnarBatch.numRows());
      return b;
    };
  }
}
