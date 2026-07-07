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

import org.apache.hudi.{HoodieUnsafeRDD, SparkAdapterSupport}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.physical.UnknownPartitioning
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.MutablePair

/**
 * Spark 3.x implementation of [[HoodieUnsafeUtils]].
 */
object Spark3HoodieUnsafeUtils extends HoodieUnsafeUtils {

  override def getNumPartitions(df: DataFrame): Int = {
    // NOTE: In general we'd rely on [[outputPartitioning]] of the executable [[SparkPlan]] to determine
    //       number of partitions plan is going to be executed with.
    //       However in case of [[LogicalRDD]] plan's output-partitioning will be stubbed as [[UnknownPartitioning]]
    //       and therefore we will be falling back to determine number of partitions by looking at the RDD itself
    df.queryExecution.logical match {
      case LogicalRDD(_, rdd, outputPartitioning, _, _) =>
        outputPartitioning match {
          case _: UnknownPartitioning => rdd.getNumPartitions
          case _ => outputPartitioning.numPartitions
        }

      case _ => df.queryExecution.executedPlan.outputPartitioning.numPartitions
    }
  }

  override def createDataFrameFrom(spark: SparkSession, plan: LogicalPlan): DataFrame =
    Dataset.ofRows(spark, plan)

  override def createDataFrameFromRows(spark: SparkSession, rows: Seq[Row], schema: StructType): DataFrame =
    Dataset.ofRows(spark, LocalRelation.fromExternalRows(
      SparkAdapterSupport.sparkAdapter.getSchemaUtils.toAttributes(schema), rows))

  override def createDataFrameFromInternalRows(spark: SparkSession, rows: Seq[InternalRow], schema: StructType): DataFrame =
    Dataset.ofRows(spark, LocalRelation(SparkAdapterSupport.sparkAdapter.getSchemaUtils.toAttributes(schema), rows))

  override def createDataFrameFromRDD(spark: SparkSession, rdd: RDD[InternalRow], schema: StructType): DataFrame =
    spark.internalCreateDataFrame(rdd, schema)

  override def collect(rdd: HoodieUnsafeRDD): Array[InternalRow] = {
    rdd.mapPartitionsInternal { iter =>
      // NOTE: We're leveraging [[MutablePair]] here to avoid unnecessary allocations, since
      //       a) iteration is performed lazily and b) iteration is single-threaded (w/in partition)
      val pair = new MutablePair[InternalRow, Null]()
      iter.map(row => pair.update(row.copy(), null))
    }
      .map(p => p._1)
      .collect()
  }
}
