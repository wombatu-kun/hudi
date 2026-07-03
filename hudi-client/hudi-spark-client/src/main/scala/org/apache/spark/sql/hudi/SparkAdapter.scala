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

package org.apache.spark.sql.hudi

import org.apache.avro.Schema
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hudi.PartitionFileSliceMapping
import org.apache.hudi.client.model.HoodieInternalRow
import org.apache.hudi.client.utils.SparkRowSerDe
import org.apache.hudi.common.model.FileSlice
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.storage.StoragePath

import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.avro.{HoodieAvroDeserializer, HoodieAvroSchemaConverters, HoodieAvroSerializer}
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, InterpretedPredicate}
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserInterface}
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.catalyst.util.DateFormatter
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.parser.HoodieExtendedParserInterface
import org.apache.spark.sql.sources.{BaseRelation, Filter}
import org.apache.spark.sql.types.{DataType, Metadata, StructType}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.UTF8String

import java.sql.{Connection, ResultSet}
import java.util.{Locale, TimeZone}

/**
 * Interface adapting discrepancies and incompatibilities between different Spark versions
 */
trait SparkAdapter extends Serializable {

  /**
   * Checks whether provided instance of [[InternalRow]] is actually an instance of [[ColumnarBatchRow]]
   */
  def isColumnarBatchRow(r: InternalRow): Boolean

  /**
   * Creates Catalyst [[Metadata]] for Hudi's meta-fields (designating these w/
   * [[METADATA_COL_ATTR_KEY]] if available (available in Spark >= 3.2)
   */
  def createCatalystMetadataForMetaField: Metadata

  /**
   * Inject table-valued functions to SparkSessionExtensions
   */
  def injectTableFunctions(extensions : SparkSessionExtensions): Unit = {}

  /**
   * Returns an instance of [[HoodieCatalogUtils]] providing for common utils operating on Spark's
   * [[TableCatalog]]s
   */
  def getCatalogUtils: HoodieCatalogUtils

  /**
   * Returns an instance of [[HoodieCatalystExpressionUtils]] providing for common utils operating
   * on Catalyst [[Expression]]s
   */
  def getCatalystExpressionUtils: HoodieCatalystExpressionUtils

  /**
   * Returns an instance of [[HoodieCatalystPlansUtils]] providing for common utils operating
   * on Catalyst [[LogicalPlan]]s
   */
  def getCatalystPlanUtils: HoodieCatalystPlansUtils

  /**
   * Returns an instance of [[HoodieSchemaUtils]] providing schema utils.
   */
  def getSchemaUtils: HoodieSchemaUtils

  /**
   * Returns an instance of [[DataFrameUtil]] for creating [[DataFrame]]s from low-level Spark
   * constructs in a Spark-version-neutral way.
   */
  def getDataFrameUtil: DataFrameUtil

  /**
   * Returns an instance of [[HoodieUnsafeUtils]] for handling [[org.apache.hudi.HoodieUnsafeRDD]]s
   * in a Spark-version-neutral way.
   */
  def getUnsafeUtils: HoodieUnsafeUtils

  /**
   * Returns an instance of [[HoodieUTF8StringFactory]] for wrapping [[org.apache.spark.unsafe.types.UTF8String]]s
   * into comparable values in a Spark-version-neutral way (Spark 4 disables UTF8String.compareTo).
   */
  def getUTF8StringFactory: HoodieUTF8StringFactory

  /**
   * Creates a [[DataFrame]] from the given [[RDD]] of [[InternalRow]]s and [[schema]], delegating to
   * Spark's (package-private) [[SparkSession#internalCreateDataFrame]], which moved packages in Spark 4.
   */
  def internalCreateDataFrame(spark: SparkSession, rdd: RDD[InternalRow], schema: StructType,
                              isStreaming: Boolean = false): DataFrame

  /**
   * Creates a Spark-version-specific concrete [[HoodieInternalRow]] overlaying the given meta-fields
   * on top of [[sourceRow]]. [[HoodieInternalRow]] is abstract because Spark 4 added an abstract
   * `getVariant` to [[org.apache.spark.sql.catalyst.expressions.SpecializedGetters]].
   */
  def createInternalRow(metaFields: Array[UTF8String],
                        sourceRow: InternalRow,
                        sourceContainsMetaFields: Boolean): HoodieInternalRow

  /**
   * Convenience overload building the meta-field array from the individual Hudi meta columns.
   */
  def createInternalRow(commitTime: UTF8String,
                        commitSeqNumber: UTF8String,
                        recordKey: UTF8String,
                        partitionPath: UTF8String,
                        fileName: UTF8String,
                        sourceRow: InternalRow,
                        sourceContainsMetaFields: Boolean): HoodieInternalRow =
    createInternalRow(Array(commitTime, commitSeqNumber, recordKey, partitionPath, fileName),
      sourceRow, sourceContainsMetaFields)

  /**
   * Creates a Spark-version-specific concrete [[PartitionFileSliceMapping]] overlaying the given
   * file-slice [[slices]] on top of the partition-values [[internalRow]]. [[PartitionFileSliceMapping]]
   * is abstract for the same reason [[HoodieInternalRow]] is: Spark 4 added an abstract `getVariant`
   * to [[org.apache.spark.sql.catalyst.expressions.SpecializedGetters]].
   */
  def createPartitionFileSliceMapping(internalRow: InternalRow,
                                      slices: Map[String, FileSlice]): PartitionFileSliceMapping

  /**
   * Returns the Catalyst schema for a JDBC [[ResultSet]] in a Spark-version-neutral way. Spark 4.0
   * added a leading [[Connection]] parameter to the underlying JdbcUtils.getSchema; the Spark 3 impl
   * ignores the connection while the Spark 4 impl forwards it.
   */
  def getJdbcSchema(connection: Connection, resultSet: ResultSet, dialect: JdbcDialect,
                    alwaysNullable: Boolean): StructType

  /**
   * Creates a [[Column]] wrapping the given Catalyst [[Expression]] in a Spark-version-neutral way.
   * Spark 4 backs [[Column]] with an [[org.apache.spark.sql.internal.ColumnNode]] rather than an
   * [[Expression]], so `new Column(expr)` no longer compiles across versions.
   */
  def createColumnFromExpression(expression: Expression): Column

  /**
   * Extracts the Catalyst [[Expression]] underlying the given [[Column]] in a Spark-version-neutral way.
   * Spark 4 removed the public `Column#expr` accessor.
   */
  def getExpressionFromColumn(column: Column): Expression

  /**
   * Creates instance of [[HoodieAvroSerializer]] providing for ability to serialize
   * Spark's [[InternalRow]] into Avro payloads
   */
  def createAvroSerializer(rootCatalystType: DataType, rootAvroType: Schema, nullable: Boolean): HoodieAvroSerializer

  /**
   * Creates instance of [[HoodieAvroDeserializer]] providing for ability to deserialize
   * Avro payloads into Spark's [[InternalRow]]
   */
  def createAvroDeserializer(rootAvroType: Schema, rootCatalystType: DataType): HoodieAvroDeserializer

  /**
   * Creates instance of [[HoodieAvroSchemaConverters]] allowing to convert b/w Avro and Catalyst schemas
   */
  def getAvroSchemaConverters: HoodieAvroSchemaConverters

  /**
   * Create the SparkRowSerDe.
   */
  def createSparkRowSerDe(schema: StructType): SparkRowSerDe

  /**
   * Create the hoodie's extended spark sql parser.
   */
  def createExtendedSparkParser(spark: SparkSession, delegate: ParserInterface): HoodieExtendedParserInterface

  /**
   * Create the SparkParsePartitionUtil.
   */
  def getSparkParsePartitionUtil: SparkParsePartitionUtil

  /**
   * Gets the [[HoodieSparkPartitionedFileUtils]].
   */
  def getSparkPartitionedFileUtils: HoodieSparkPartitionedFileUtils

  /**
   * Get the [[DateFormatter]].
   */
  def getDateFormatter(tz: TimeZone): DateFormatter

  /**
   * Combine [[PartitionedFile]] to [[FilePartition]] according to `maxSplitBytes`.
   */
  def getFilePartitions(sparkSession: SparkSession, partitionedFiles: Seq[PartitionedFile],
      maxSplitBytes: Long): Seq[FilePartition]

  /**
   * Checks whether [[LogicalPlan]] refers to Hudi table, and if it's the case extracts
   * corresponding [[CatalogTable]]
   */
  def resolveHoodieTable(plan: LogicalPlan): Option[CatalogTable] = {
    EliminateSubqueryAliases(plan) match {
      // First, we need to weed out unresolved plans
      case plan if !plan.resolved => None
      // NOTE: When resolving Hudi table we allow [[Filter]]s and [[Project]]s be applied
      //       on top of it
      // NOTE: matched positionally-agnostically ([[LogicalRelation]] gained a `stream` field in
      //       Spark 4), extracting the catalog table through the stable `catalogTable` accessor
      case PhysicalOperation(_, _, lr: LogicalRelation) if lr.catalogTable.exists(isHoodieTable) => lr.catalogTable
      case _ => None
    }
  }

  def isHoodieTable(map: java.util.Map[String, String]): Boolean = {
    isHoodieTable(map.getOrDefault("provider", ""))
  }

  def isHoodieTable(table: CatalogTable): Boolean = {
    isHoodieTable(table.provider.map(_.toLowerCase(Locale.ROOT)).orNull)
  }

  def isHoodieTable(tableId: TableIdentifier, spark: SparkSession): Boolean = {
    val table = spark.sessionState.catalog.getTableMetadata(tableId)
    isHoodieTable(table)
  }

  def isHoodieTable(provider: String): Boolean = {
    "hudi".equalsIgnoreCase(provider)
  }

  /**
   * Create instance of [[ParquetFileFormat]]
   */
  def createLegacyHoodieParquetFileFormat(appendPartitionValues: Boolean): Option[ParquetFileFormat]

  def makeColumnarBatch(vectors: Array[ColumnVector], numRows: Int): ColumnarBatch

  /**
   * Create instance of [[InterpretedPredicate]]
   *
   * TODO move to HoodieCatalystExpressionUtils
   */
  def createInterpretedPredicate(e: Expression): InterpretedPredicate

  /**
   * Create Hoodie relation based on globPaths, otherwise use tablePath if it's empty
   */
  def createRelation(sqlContext: SQLContext,
                     metaClient: HoodieTableMetaClient,
                     schema: Schema,
                     globPaths: Array[StoragePath],
                     parameters: java.util.Map[String, String]): BaseRelation

  /**
   * Create instance of [[HoodieFileScanRDD]]
   * SPARK-37273 FileScanRDD constructor changed in SPARK 3.3
   */
  def createHoodieFileScanRDD(sparkSession: SparkSession,
                              readFunction: PartitionedFile => Iterator[InternalRow],
                              filePartitions: Seq[FilePartition],
                              readDataSchema: StructType,
                              metadataColumns: Seq[AttributeReference] = Seq.empty): FileScanRDD

  /**
   * Extract condition in [[DeleteFromTable]]
   * SPARK-38626 condition is no longer Option in Spark 3.3
   */
  def extractDeleteCondition(deleteFromTable: Command): Expression

  /**
   * Converts instance of [[StorageLevel]] to a corresponding string
   */
  def convertStorageLevelToString(level: StorageLevel): String

  /**
   * Tries to translate a Catalyst Expression into data source Filter
   */
  def translateFilter(predicate: Expression, supportNestedPredicatePushdown: Boolean = false): Option[Filter]

  /**
   * Creates a [[ParseException]] carrying proper location information. Abstracts over the constructor
   * differences between Spark 3.x (message-based) and Spark 4.x (error-class-based).
   */
  def newParseException(command: Option[String],
                        exception: AnalysisException,
                        start: Origin,
                        stop: Origin): ParseException
}
