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

package org.apache.spark.sql.adapter

import org.apache.hudi.client.model.{HoodieInternalRow, Spark4HoodieInternalRow}
import org.apache.hudi.client.utils.SparkRowSerDe
import org.apache.hudi.common.model.FileSlice
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.JsonUtils
import org.apache.hudi.spark4.internal.ReflectUtil
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.{AvroConversionUtils, DefaultSource, HoodieSparkUtils, PartitionFileSliceMapping, Spark4PartitionFileSliceMapping, Spark4RowSerDe}

import org.apache.avro.Schema
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.avro.{HoodieAvroSchemaConverters, HoodieSparkAvroSchemaConverters}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, InterpretedPredicate, Predicate}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.catalyst.util.DateFormatter
import org.apache.spark.sql.classic.ColumnConversions
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.hudi.SparkAdapter
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.sources.{BaseRelation, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, DataFrameUtil, ExpressionColumnNodeWrapper, HoodieSpark4CatalogUtils, HoodieUnsafeUtils, HoodieUTF8StringFactory, SQLContext, Spark4DataFrameUtil, Spark4HoodieUnsafeUtils, Spark4HoodieUTF8StringFactory, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.UTF8String

import java.sql.{Connection, ResultSet}
import java.time.ZoneId
import java.util.TimeZone
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

/**
 * Base implementation of [[SparkAdapter]] for Spark 3.x branch
 */
abstract class BaseSpark4Adapter extends SparkAdapter with Logging {

  // JsonUtils for Support Spark Version >= 3.3
  if (HoodieSparkUtils.gteqSpark3_3) JsonUtils.registerModules()

  private val cache = new ConcurrentHashMap[ZoneId, DateFormatter](1)

  def getCatalogUtils: HoodieSpark4CatalogUtils

  override def createSparkRowSerDe(schema: StructType): SparkRowSerDe = {
    new Spark4RowSerDe(getCatalystExpressionUtils.getEncoder(schema))
  }

  override def getAvroSchemaConverters: HoodieAvroSchemaConverters = HoodieSparkAvroSchemaConverters

  override def getSparkParsePartitionUtil: SparkParsePartitionUtil = Spark4ParsePartitionUtil

  override def getDateFormatter(tz: TimeZone): DateFormatter = {
    cache.computeIfAbsent(tz.toZoneId, zoneId => ReflectUtil.getDateFormatter(zoneId))
  }

  /**
   * Combine [[PartitionedFile]] to [[FilePartition]] according to `maxSplitBytes`.
   */
  override def getFilePartitions(
      sparkSession: SparkSession,
      partitionedFiles: Seq[PartitionedFile],
      maxSplitBytes: Long): Seq[FilePartition] = {
    FilePartition.getFilePartitions(sparkSession, partitionedFiles, maxSplitBytes)
  }

  override def createInterpretedPredicate(e: Expression): InterpretedPredicate = {
    Predicate.createInterpreted(e)
  }

  override def createRelation(sqlContext: SQLContext,
                              metaClient: HoodieTableMetaClient,
                              schema: Schema,
                              globPaths: Array[StoragePath],
                              parameters: java.util.Map[String, String]): BaseRelation = {
    val dataSchema = Option(schema).map(AvroConversionUtils.convertAvroSchemaToStructType).orNull
    DefaultSource.createRelation(sqlContext, metaClient, dataSchema, globPaths, parameters.asScala.toMap)
  }

  override def convertStorageLevelToString(level: StorageLevel): String

  override def translateFilter(predicate: Expression,
                               supportNestedPredicatePushdown: Boolean = false): Option[Filter] = {
    DataSourceStrategy.translateFilter(predicate, supportNestedPredicatePushdown)
  }

  override def makeColumnarBatch(vectors: Array[ColumnVector], numRows: Int): ColumnarBatch = {
    new ColumnarBatch(vectors, numRows)
  }

  override def getDataFrameUtil: DataFrameUtil = Spark4DataFrameUtil

  override def getUnsafeUtils: HoodieUnsafeUtils = Spark4HoodieUnsafeUtils

  override def getUTF8StringFactory: HoodieUTF8StringFactory = Spark4HoodieUTF8StringFactory

  override def internalCreateDataFrame(spark: SparkSession,
                                       rdd: RDD[InternalRow],
                                       schema: StructType,
                                       isStreaming: Boolean = false): DataFrame =
    spark.asInstanceOf[org.apache.spark.sql.classic.SparkSession].internalCreateDataFrame(rdd, schema, isStreaming)

  override def createInternalRow(metaFields: Array[UTF8String],
                                 sourceRow: InternalRow,
                                 sourceContainsMetaFields: Boolean): HoodieInternalRow =
    new Spark4HoodieInternalRow(metaFields, sourceRow, sourceContainsMetaFields)

  override def createPartitionFileSliceMapping(internalRow: InternalRow,
                                               slices: Map[String, FileSlice]): PartitionFileSliceMapping =
    new Spark4PartitionFileSliceMapping(internalRow, slices)

  override def getJdbcSchema(connection: Connection, resultSet: ResultSet, dialect: JdbcDialect,
                             alwaysNullable: Boolean): StructType =
    JdbcUtils.getSchema(connection, resultSet, dialect, alwaysNullable, false)

  override def createColumnFromExpression(expression: Expression): Column =
    new Column(ExpressionColumnNodeWrapper.apply(expression))

  override def getExpressionFromColumn(column: Column): Expression =
    ColumnConversions.expression(column)

  override def newParseException(command: Option[String],
                                 exception: AnalysisException,
                                 start: Origin,
                                 stop: Origin): ParseException = {
    new ParseException(command, start, stop, exception.getErrorClass, exception.getMessageParameters.asScala.toMap)
  }
}
