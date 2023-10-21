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

import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.ttl.model.{TtlPolicy, TtlPolicyLevel}
import org.apache.hudi.table.ttl.TtlPolicyService
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.time.temporal.ChronoUnit
import java.util
import java.util.Collections
import java.util.function.Supplier
import scala.collection.JavaConversions._

class TtlPolicySaveProcedure() extends BaseProcedure with ProcedureBuilder {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "path", DataTypes.StringType),
    ProcedureParameter.required(2, "spec", DataTypes.StringType),
    ProcedureParameter.required(3, "level", DataTypes.StringType),
    ProcedureParameter.required(4, "value", DataTypes.StringType),
    ProcedureParameter.required(5, "units", DataTypes.StringType)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("partition", DataTypes.StringType, nullable = false, Metadata.empty),
    StructField("lastUpdate", DataTypes.StringType, nullable = false, Metadata.empty),
    StructField("spec", DataTypes.StringType, nullable = false, Metadata.empty),
    StructField("level", DataTypes.StringType, nullable = false, Metadata.empty),
    StructField("value", DataTypes.StringType, nullable = false, Metadata.empty),
    StructField("units", DataTypes.StringType, nullable = false, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)
    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getArgValueOrDefault(args, PARAMETERS(1))
    val basePath = getBasePath(tableName, tablePath)
    val spec = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[String]
    val level = getArgValueOrDefault(args, PARAMETERS(3)).get.asInstanceOf[String]
    val value = getArgValueOrDefault(args, PARAMETERS(4)).get.asInstanceOf[String]
    val units = getArgValueOrDefault(args, PARAMETERS(5)).get.asInstanceOf[String]
    val policy = new TtlPolicy(spec, TtlPolicyLevel.valueOf(level), Integer.valueOf(value), ChronoUnit.valueOf(units))
    val metaClient = HoodieTableMetaClient.builder.setConf(jsc.hadoopConfiguration()).setBasePath(basePath).build
    metaClient.getTtlPolicyDAO.save(policy)

    val rows = new util.ArrayList[Row]
    rows.add(Row("POLICY", "SAVED", spec, level, value, units))
    rows.add(Row("if run it", "immediately", "will", "delete", "partitions", "(sample):"))
    getTtlPolicyService(tableName, tablePath)
      .findExpiredPartitions(Collections.singletonList(policy), TtlPolicyService.DELETION_SAMPLE_SIZE)
      .foreach(p => rows.add(Row(p.getSource, p.getLastUpdate, p.getSpec, p.getLevel.toString, String.valueOf(p.getValue), p.getUnits.toString)))

    rows.stream().toArray().map(r => r.asInstanceOf[Row]).toList
  }

  override def build: Procedure = new TtlPolicySaveProcedure()

}

object TtlPolicySaveProcedure {
  val NAME = "ttl_policy_save"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new TtlPolicySaveProcedure()
  }
}
