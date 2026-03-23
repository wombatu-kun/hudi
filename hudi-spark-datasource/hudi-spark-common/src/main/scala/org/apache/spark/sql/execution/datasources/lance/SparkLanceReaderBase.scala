/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.execution.datasources.lance

import org.apache.hudi.SparkAdapterSupport.sparkAdapter
import org.apache.hudi.common.util
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.io.memory.HoodieArrowAllocator
import org.apache.hudi.io.storage.{HoodieSparkLanceReader, LanceBatchIterator, LanceRecordIterator}
import org.apache.hudi.storage.StorageConfiguration

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.schema.MessageType
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, JoinedRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.datasources.{PartitionedFile, SparkColumnarFileReader, SparkSchemaTransformUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.LanceArrowUtils
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnarBatchUtils, ColumnVector}
import org.lance.file.LanceFileReader

import java.io.IOException

import scala.collection.JavaConverters._

/**
 * Reader for Lance files in Spark datasource.
 *
 * When {@code enableVectorizedReader} is true and there are no implicit type changes (schema
 * evolution requiring casts), the reader returns {@link ColumnarBatch} objects directly, taking
 * full advantage of Lance's Arrow-native columnar storage.  Missing columns are null-padded and
 * partition values are appended as constant column vectors via {@link ColumnarBatchUtils},
 * matching the pattern used by the Parquet and ORC vectorized readers.
 *
 * When {@code enableVectorizedReader} is false or implicit type changes are required, the reader
 * falls back to the row-based path using {@link LanceRecordIterator}, which applies
 * {@link UnsafeProjection} for type casting and schema evolution.
 *
 * @param enableVectorizedReader whether to use columnar batch reading
 */
class SparkLanceReaderBase(enableVectorizedReader: Boolean) extends SparkColumnarFileReader {

  // Batch size for reading Lance files (number of rows per batch)
  private val DEFAULT_BATCH_SIZE = 512

  /**
   * Read a Lance file with schema projection and partition column support.
   *
   * @param file              Lance file to read
   * @param requiredSchema    desired output schema of the data (columns to read)
   * @param partitionSchema   schema of the partition columns; values are appended to every row/batch
   * @param internalSchemaOpt option of internal schema for schema-on-read (not currently used for Lance)
   * @param filters           filters for data skipping (not yet pushed down to Lance)
   * @param storageConf       the hadoop conf
   * @return iterator of rows or batches; declared as [[InternalRow]] but may contain [[ColumnarBatch]]
   */
  override def read(file: PartitionedFile,
                    requiredSchema: StructType,
                    partitionSchema: StructType,
                    internalSchemaOpt: util.Option[InternalSchema],
                    filters: scala.Seq[Filter],
                    storageConf: StorageConfiguration[Configuration],
                    tableSchemaOpt: util.Option[MessageType] = util.Option.empty()): Iterator[InternalRow] = {

    val filePath = file.filePath.toString

    if (requiredSchema.isEmpty && partitionSchema.isEmpty) {
      // No columns requested - return empty iterator
      Iterator.empty
    } else {
      // Track iterator for cleanup
      var lanceIterator: LanceRecordIterator = null
      var batchIterator: LanceBatchIterator = null

      // Create child allocator for reading
      val allocator = HoodieArrowAllocator.newChildAllocator(getClass.getSimpleName + "-data-" + filePath,
        HoodieSparkLanceReader.LANCE_DATA_ALLOCATOR_SIZE);

      try {
        // Open Lance file reader
        val lanceReader = LanceFileReader.open(filePath, allocator)

        // Get schema from Lance file
        val arrowSchema = lanceReader.schema()
        val fileSchema = LanceArrowUtils.fromArrowSchema(arrowSchema)

        // Build type change info for schema evolution
        val (implicitTypeChangeInfo, sparkRequestSchema) =
          SparkSchemaTransformUtils.buildImplicitSchemaChangeInfo(fileSchema, requiredSchema)

        // Filter schema to only fields that exist in file (Lance can only read columns present in file)
        val requestSchema = SparkSchemaTransformUtils.filterSchemaByFileSchema(sparkRequestSchema, fileSchema)

        val columnNames = if (requestSchema.nonEmpty) {
          requestSchema.fieldNames.toList.asJava
        } else {
          // If only partition columns requested, read minimal data
          null
        }

        // Read data with column projection (filters not supported yet)
        val arrowReader = lanceReader.readAll(columnNames, null, DEFAULT_BATCH_SIZE)

        if (enableVectorizedReader && implicitTypeChangeInfo.isEmpty) {
          // ---- Columnar batch path ----
          // Lance is Arrow-native, so we surface ColumnarBatch directly without row conversion.
          // Implicit type changes (schema evolution requiring casts) are handled by the row path only.
          batchIterator = new LanceBatchIterator(allocator, lanceReader, arrowReader, filePath)

          Option(TaskContext.get()).foreach { ctx =>
            ctx.addTaskCompletionListener[Unit](_ => batchIterator.close())
          }

          batchIterator.asScala.map { batch =>
            projectBatch(batch, requestSchema, requiredSchema, partitionSchema, file.partitionValues)
          }.asInstanceOf[Iterator[InternalRow]]

        } else {
          // ---- Row-based path ----
          // Used when vectorized reading is disabled or implicit type changes require row-level casts.
          // Create iterator using shared LanceRecordIterator
          lanceIterator = new LanceRecordIterator(
            allocator,
            lanceReader,
            arrowReader,
            requestSchema,
            filePath
          )

          // Register cleanup listener
          Option(TaskContext.get()).foreach { ctx =>
            ctx.addTaskCompletionListener[Unit](_ => lanceIterator.close())
          }

          // Create the following projections for schema evolution:
          // 1. Padding projection: add NULL for missing columns
          // 2. Casting projection: handle type conversions
          val schemaUtils = sparkAdapter.getSchemaUtils
          val paddingProj = SparkSchemaTransformUtils.generateNullPaddingProjection(requestSchema, requiredSchema)
          val castProj = SparkSchemaTransformUtils.generateUnsafeProjection(
            schemaUtils.toAttributes(requiredSchema),
            Some(SQLConf.get.sessionLocalTimeZone),
            implicitTypeChangeInfo,
            requiredSchema,
            new StructType(),
            schemaUtils
          )

          // Unify projections by applying padding and then casting for each row
          val projection: UnsafeProjection = new UnsafeProjection {
            def apply(row: InternalRow): UnsafeRow =
              castProj(paddingProj(row))
          }
          val projectedIter = lanceIterator.asScala.map(projection.apply)

          // Handle partition columns
          if (partitionSchema.length == 0) {
            // No partition columns - return rows directly
            projectedIter
          } else {
            // Create UnsafeProjection to convert JoinedRow to UnsafeRow
            val fullSchema = (requiredSchema.fields ++ partitionSchema.fields).map(f =>
              AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
            val unsafeProjection = GenerateUnsafeProjection.generate(fullSchema, fullSchema)

            // Append partition values to each row using JoinedRow, then convert to UnsafeRow
            val joinedRow = new JoinedRow()
            projectedIter.map(row => unsafeProjection(joinedRow(row, file.partitionValues)))
          }
        }

      } catch {
        case e: Exception =>
          if (batchIterator != null) {
            batchIterator.close()
          } else if (lanceIterator != null) {
            lanceIterator.close()
          } else {
            allocator.close()
          }
          throw new IOException(s"Failed to read Lance file: $filePath", e)
      }
    }
  }

  /**
   * Project a raw Lance {@link ColumnarBatch} into the desired output schema.
   *
   * For each field in {@code requiredSchema}:
   *   - If the field exists in {@code requestSchema} (read from the file), reuse the corresponding
   *     {@link ColumnVector} from the source batch.
   *   - If the field is absent (e.g. newly added column), create a null constant vector via
   *     {@link ColumnarBatchUtils#createNullVector}.
   *
   * Partition columns are appended as constant vectors via
   * {@link ColumnarBatchUtils#createPartitionVector}.
   *
   * The returned {@link ColumnarBatch} shares underlying Arrow buffers with the source batch for
   * existing columns — it must not outlive the source batch.
   */
  private def projectBatch(sourceBatch: ColumnarBatch,
                           requestSchema: StructType,
                           requiredSchema: StructType,
                           partitionSchema: StructType,
                           partitionValues: InternalRow): ColumnarBatch = {
    val numRows = sourceBatch.numRows()

    val dataVectors: Array[ColumnVector] = requiredSchema.fields.map { field =>
      if (requestSchema.fieldNames.contains(field.name)) {
        sourceBatch.column(requestSchema.fieldIndex(field.name))
      } else {
        // Java helper returns ColumnVector directly — keeps ConstantColumnVector out of Scala
        // inference, avoiding the scala.<:< implicit search that crashes IntelliJ's Scala daemon
        ColumnarBatchUtils.createNullVector(numRows, field.dataType)
      }
    }

    val partVectors: Array[ColumnVector] = partitionSchema.fields.zipWithIndex.map { case (field, i) =>
      ColumnarBatchUtils.createPartitionVector(numRows, field.dataType, partitionValues, i)
    }

    val result = new ColumnarBatch(dataVectors ++ partVectors)
    result.setNumRows(numRows)
    result
  }
}

