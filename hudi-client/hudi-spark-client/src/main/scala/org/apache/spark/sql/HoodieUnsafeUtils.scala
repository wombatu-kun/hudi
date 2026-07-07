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

import org.apache.hudi.HoodieUnsafeRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.StructType

/**
 * Suite of utilities helping in handling instances of [[HoodieUnsafeRDD]].
 *
 * The concrete implementation is Spark-version specific (see the Spark3/Spark4 variants in the
 * per-version modules): the [[Dataset]] factory, [[SparkSession#internalCreateDataFrame]] and the
 * [[org.apache.spark.sql.execution.LogicalRDD]] shape all changed in Spark 4. Obtain the active
 * implementation through [[org.apache.spark.sql.hudi.SparkAdapter#getUnsafeUtils]].
 */
trait HoodieUnsafeUtils {

  /**
   * Fetches expected number of output partitions of the provided [[DataFrame]].
   */
  def getNumPartitions(df: DataFrame): Int

  /**
   * Creates [[DataFrame]] from provided [[plan]].
   */
  def createDataFrameFrom(spark: SparkSession, plan: LogicalPlan): DataFrame

  /**
   * Creates [[DataFrame]] from the in-memory [[Seq]] of [[Row]]s with provided [[schema]].
   */
  def createDataFrameFromRows(spark: SparkSession, rows: Seq[Row], schema: StructType): DataFrame

  /**
   * Creates [[DataFrame]] from the in-memory [[Seq]] of [[InternalRow]]s with provided [[schema]].
   */
  def createDataFrameFromInternalRows(spark: SparkSession, rows: Seq[InternalRow], schema: StructType): DataFrame

  /**
   * Creates [[DataFrame]] from the [[RDD]] of [[InternalRow]]s with provided [[schema]].
   */
  def createDataFrameFromRDD(spark: SparkSession, rdd: RDD[InternalRow], schema: StructType): DataFrame

  /**
   * Canonical implementation of the [[org.apache.spark.rdd.RDD#collect]] for [[HoodieUnsafeRDD]],
   * returning a properly copied [[Array]] of [[InternalRow]]s.
   */
  def collect(rdd: HoodieUnsafeRDD): Array[InternalRow]
}
