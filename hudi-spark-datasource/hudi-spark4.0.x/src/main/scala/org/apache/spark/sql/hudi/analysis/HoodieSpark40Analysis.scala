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

package org.apache.spark.sql.hudi.analysis

import org.apache.hudi.{DefaultSource, SparkAdapterSupport}
import org.apache.hudi.common.model.HoodieRecord

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hudi.ProvidesHoodieConfig
import org.apache.spark.sql.hudi.catalog.HoodieInternalV2Table
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
 * NOTE: PLEASE READ CAREFULLY
 *
 * Since Hudi relations don't currently implement DS V2 Read API, we have to fallback to V1 here.
 * Such fallback will have considerable performance impact, therefore it's only performed in cases
 * where V2 API have to be used. Currently only such use-case is using of Schema Evolution feature
 *
 * Check out HUDI-4178 for more details
 */
case class HoodieSpark40DataSourceV2ToV1Fallback(sparkSession: SparkSession) extends Rule[LogicalPlan]
  with ProvidesHoodieConfig with SparkAdapterSupport {

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    // The only place we're avoiding fallback is in [[AlterTableCommand]]s since
    // current implementation relies on DSv2 features
    case _: AlterTableCommand => plan

    // NOTE: Unfortunately, [[InsertIntoStatement]] is implemented in a way that doesn't expose
    //       target relation as a child (even though there's no good reason for that)
    //
    //       An INSERT carrying an explicit column list keeps its [[userSpecifiedCols]], whereas Hudi's
    //       early [[ResolveImplementationsEarly]] conversion drops them -- which preempts Spark's own
    //       column-DEFAULT resolution ([[ResolveInsertInto]]) and leaves omitted columns unfilled
    //       (SPARK INSERT_COLUMN_ARITY_MISMATCH). We replicate that resolution before the column list is
    //       dropped, expanding the query into a full-width projection padded with the declared DEFAULTs.
    case iis@InsertIntoStatement(rv2@DataSourceV2Relation(v2Table: HoodieInternalV2Table, _, _, _, _), _, _, _, _, _, _) =>
      fillMissingColumnsWithDefaults(iis, v2Table.v1Table).copy(table = convertToV1(rv2, v2Table))

    // Handles the case where the insert query only becomes resolved after this rule already ran the
    // DSv2 -> DSv1 fallback above (so the target is now a [[LogicalRelation]] while the column list is
    // still present). The same expansion still has to happen before the column list is dropped.
    case iis@InsertIntoStatement(lr: LogicalRelation, _, _, _, _, _, _)
        if iis.userSpecifiedCols.nonEmpty && iis.query.resolved =>
      sparkAdapter.resolveHoodieTable(lr)
        .map(fillMissingColumnsWithDefaults(iis, _))
        .getOrElse(iis)

    case _ =>
      plan.resolveOperatorsDown {
        case rv2@DataSourceV2Relation(v2Table: HoodieInternalV2Table, _, _, _, _) => convertToV1(rv2, v2Table)
      }
  }

  /**
   * Expands an INSERT with an explicit (partial) column list into a full-width projection over the
   * table schema, mapping the user-provided columns by name and filling every omitted column with its
   * declared DEFAULT value (or NULL literal for nullable columns without a default). The provided
   * [[catalogTable]] schema carries the column DEFAULT metadata written at CREATE time.
   */
  private def fillMissingColumnsWithDefaults(iis: InsertIntoStatement,
                                             catalogTable: CatalogTable): InsertIntoStatement = {
    // Only rewrite explicit-column-list inserts into non-partitioned tables whose query is already
    // resolved (otherwise there is no output to map the provided columns onto). All other inserts are
    // left untouched.
    if (iis.userSpecifiedCols.isEmpty || !iis.query.resolved ||
      catalogTable.partitionColumnNames.nonEmpty ||
      iis.userSpecifiedCols.length != iis.query.output.length) {
      iis
    } else {
      val resolver = sparkSession.sessionState.conf.resolver
      val provided = iis.userSpecifiedCols.zip(iis.query.output)
      val dataColumns = catalogTable.schema.filterNot(f => HoodieRecord.HOODIE_META_COLUMNS.contains(f.name))
      val projectList: Seq[NamedExpression] = dataColumns.map { field =>
        provided.collectFirst {
          case (name, attr) if resolver(name, field.name) => Alias(attr, field.name)()
        }.getOrElse {
          ResolveDefaultColumns.getDefaultValueExprOrNullLit(field) match {
            case named: NamedExpression => named
            case expr => Alias(expr, field.name)()
          }
        }
      }
      iis.copy(query = Project(projectList, iis.query), userSpecifiedCols = Nil)
    }
  }

  private def convertToV1(rv2: DataSourceV2Relation, v2Table: HoodieInternalV2Table) = {
    val output = rv2.output
    val catalogTable = v2Table.catalogTable.map(_ => v2Table.v1Table)
    val relation = new DefaultSource().createRelation(sparkSession.sqlContext,
      buildHoodieConfig(v2Table.hoodieCatalogTable), v2Table.hoodieCatalogTable.tableSchema)

    LogicalRelation(relation, output, catalogTable, isStreaming = false, Option.empty)
  }
}
