/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.StructType

/**
 * Utilities for building a [[DataFrame]] out of an RDD[InternalRow] such as the one obtained via
 * `df.queryExecution.toRdd`.
 *
 * The concrete implementation is Spark-version specific (see the Spark3/Spark4 variants in the
 * per-version modules), because the underlying [[Dataset]] factory moved packages in Spark 4.
 * Obtain the active implementation through [[org.apache.spark.sql.hudi.SparkAdapter#getDataFrameUtil]].
 */
trait DataFrameUtil {
  def createFromInternalRows(sparkSession: SparkSession, schema: StructType, rdd: RDD[InternalRow]): DataFrame

  /**
   * Builds a [[DataFrame]] out of an analyzed [[LogicalPlan]]. This is the Spark-version-safe
   * replacement for a direct `Dataset.ofRows(spark, plan)` call, whose `Dataset` companion moved to
   * the `org.apache.spark.sql.classic` package in Spark 4.
   */
  def ofRows(sparkSession: SparkSession, logicalPlan: LogicalPlan): DataFrame
}
