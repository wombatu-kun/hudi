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

import org.apache.hudi.SparkAdapterSupport
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.classic.{Dataset => ClassicDataset, SparkSession => ClassicSparkSession}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.types.StructType

/**
 * Spark 4.x implementation of [[DataFrameUtil]]. Spark 4 relocated `Dataset.ofRows` and the concrete
 * [[SparkSession]] to the `org.apache.spark.sql.classic` package (the Spark Connect split).
 */
object Spark4DataFrameUtil extends DataFrameUtil {

  override def createFromInternalRows(sparkSession: SparkSession, schema: StructType,
                                      rdd: RDD[InternalRow]): DataFrame = {
    val logicalPlan = LogicalRDD(
      SparkAdapterSupport.sparkAdapter.getSchemaUtils.toAttributes(schema),
      rdd)(sparkSession.asInstanceOf[ClassicSparkSession])
    ClassicDataset.ofRows(sparkSession.asInstanceOf[ClassicSparkSession], logicalPlan)
  }

  override def ofRows(sparkSession: SparkSession, logicalPlan: LogicalPlan): DataFrame =
    ClassicDataset.ofRows(sparkSession.asInstanceOf[ClassicSparkSession], logicalPlan)
}
