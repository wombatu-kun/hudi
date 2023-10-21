/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.command.procedures

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util
import java.util.function.Supplier
import scala.collection.JavaConversions._

class TtlUpdatePartitionsProcedure() extends BaseProcedure with ProcedureBuilder {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "path", DataTypes.StringType),
    ProcedureParameter.required(2, "dryRun", DataTypes.BooleanType)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("partition", DataTypes.StringType, nullable = false, Metadata.empty),
    StructField("lastUpdateTime", DataTypes.StringType, nullable = false, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)
    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getArgValueOrDefault(args, PARAMETERS(1))
    val dryRun = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[Boolean]

    val rows = new util.ArrayList[Row]
    getTtlPolicyService(tableName, tablePath)
      .updatePartitionsMetadata(dryRun)
      .foreach(p => rows.add(Row(p._1, p._2)))

    rows.stream().toArray().map(r => r.asInstanceOf[Row]).toList
  }

  override def build: Procedure = new TtlUpdatePartitionsProcedure()

}

object TtlUpdatePartitionsProcedure {
  val NAME = "ttl_update_partitions"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new TtlUpdatePartitionsProcedure()
  }
}
