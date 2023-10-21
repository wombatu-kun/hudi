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

import org.apache.hudi.common.table.ttl.TtlConfigurer
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util
import java.util.function.Supplier
import scala.collection.JavaConversions._

class TtlConfigurationProcedure() extends BaseProcedure with ProcedureBuilder {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "path", DataTypes.StringType),
    ProcedureParameter.optional(2, "enabled", DataTypes.StringType),
    ProcedureParameter.optional(3, "strategy", DataTypes.StringType),
    ProcedureParameter.optional(4, "value", DataTypes.StringType),
    ProcedureParameter.optional(5, "resolveConflictsBy", DataTypes.StringType)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("key", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("value", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getArgValueOrDefault(args, PARAMETERS(1))
    val enabled = getArgValueOrDefault(args, PARAMETERS(2)).orNull.asInstanceOf[String]
    val strategy = getArgValueOrDefault(args, PARAMETERS(3)).orNull.asInstanceOf[String]
    val value = getArgValueOrDefault(args, PARAMETERS(4)).orNull.asInstanceOf[String]
    val resolveConflictsBy = getArgValueOrDefault(args, PARAMETERS(5)).orNull.asInstanceOf[String]

    val basePath = getBasePath(tableName, tablePath)
    val metaClient = HoodieTableMetaClient.builder.setConf(jsc.hadoopConfiguration()).setBasePath(basePath).build
    val updatedProps = TtlConfigurer.setAndGetTtlConf(metaClient.getFs, metaClient.getMetaPath,
      enabled, strategy, value, resolveConflictsBy)

    if (updatedProps.isEmpty) {
      val tableConfig = metaClient.getTableConfig
      updatedProps.setProperty(HoodieTableConfig.TTL_POLICIES_ENABLED.key(),
        tableConfig.getStringOrDefault(HoodieTableConfig.TTL_POLICIES_ENABLED))
      updatedProps.setProperty(HoodieTableConfig.TTL_TRIGGER_STRATEGY.key,
        tableConfig.getStringOrDefault(HoodieTableConfig.TTL_TRIGGER_STRATEGY))
      updatedProps.setProperty(HoodieTableConfig.TTL_TRIGGER_VALUE.key(),
        tableConfig.getStringOrDefault(HoodieTableConfig.TTL_TRIGGER_VALUE))
      updatedProps.setProperty(HoodieTableConfig.TTL_POLICIES_CONFLICT_RESOLUTION_RULE.key,
        tableConfig.getStringOrDefault(HoodieTableConfig.TTL_POLICIES_CONFLICT_RESOLUTION_RULE))
    }

    val rows = new util.ArrayList[Row]
    updatedProps.foreach(p => rows.add(Row(p._1, p._2)))
    rows.stream().toArray().map(r => r.asInstanceOf[Row]).toList
  }

  override def build: Procedure = new TtlConfigurationProcedure()

}

object TtlConfigurationProcedure {
  val NAME = "ttl_configuration"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new TtlConfigurationProcedure()
  }
}
