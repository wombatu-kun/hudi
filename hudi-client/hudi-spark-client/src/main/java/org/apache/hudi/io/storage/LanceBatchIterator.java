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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.LanceArrowColumnVector;
import org.lance.file.LanceFileReader;

import java.io.IOException;

/**
 * Iterator that reads Lance files and returns Arrow batches as {@link ColumnarBatch} directly,
 * enabling vectorized columnar processing without row-by-row conversion.
 *
 * <p>Each call to {@link #next()} returns a {@link ColumnarBatch} backed by
 * {@link LanceArrowColumnVector} wrappers over the live Arrow {@link VectorSchemaRoot}.
 * The caller must consume the batch fully before calling {@link #hasNext()} again, because
 * the next {@code loadNextBatch()} call overwrites the underlying Arrow buffers in-place.
 * This matches the standard Spark columnar batch iterator contract.
 *
 * <p>The iterator manages the full lifecycle of:
 * <ul>
 *   <li>{@link BufferAllocator} - Arrow memory management</li>
 *   <li>{@link LanceFileReader} - Lance file handle</li>
 *   <li>{@link ArrowReader} - Arrow batch reader</li>
 *   <li>{@link ColumnarBatch} - current batch (closed by the next {@link #hasNext()} call)</li>
 * </ul>
 */
public class LanceBatchIterator implements ClosableIterator<ColumnarBatch> {

  private final BufferAllocator allocator;
  private final LanceFileReader lanceReader;
  private final ArrowReader arrowReader;
  private final String path;

  /**
   * Cached wrappers around Arrow FieldVectors. Allocated once on the first batch and reused,
   * because Arrow reuses the same FieldVector objects in-place across batches.
   */
  private ColumnVector[] columnVectors;
  private ColumnarBatch currentBatch;
  private boolean closed = false;

  /**
   * Creates a new Lance batch iterator.
   *
   * @param allocator   Arrow buffer allocator for memory management
   * @param lanceReader Lance file reader (owns the file handle)
   * @param arrowReader Arrow reader for batch reading
   * @param path        file path used in error messages
   */
  public LanceBatchIterator(BufferAllocator allocator,
                            LanceFileReader lanceReader,
                            ArrowReader arrowReader,
                            String path) {
    this.allocator = allocator;
    this.lanceReader = lanceReader;
    this.arrowReader = arrowReader;
    this.path = path;
  }

  @Override
  public boolean hasNext() {
    // Close previous batch before loading next — Arrow reuses the underlying buffers
    if (currentBatch != null) {
      currentBatch.close();
      currentBatch = null;
    }

    try {
      if (arrowReader.loadNextBatch()) {
        VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();

        // Wrap each Arrow FieldVector in LanceArrowColumnVector for type-safe Spark access.
        // The wrappers are allocated once and reused because Arrow keeps the same FieldVector
        // objects across batches, updating their contents in place.
        if (columnVectors == null) {
          columnVectors = root.getFieldVectors().stream()
              .map(LanceArrowColumnVector::new)
              .toArray(ColumnVector[]::new);
        }

        currentBatch = new ColumnarBatch(columnVectors, root.getRowCount());
        return true;
      }
    } catch (IOException e) {
      throw new HoodieException("Failed to read next batch from Lance file: " + path, e);
    }

    return false;
  }

  @Override
  public ColumnarBatch next() {
    if (currentBatch == null) {
      throw new IllegalStateException("No batch available; call hasNext() first");
    }
    return currentBatch;
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;

    if (currentBatch != null) {
      currentBatch.close();
      currentBatch = null;
    }

    IOException arrowException = null;
    Exception lanceException = null;

    if (arrowReader != null) {
      try {
        arrowReader.close();
      } catch (IOException e) {
        arrowException = e;
      }
    }

    if (lanceReader != null) {
      try {
        lanceReader.close();
      } catch (Exception e) {
        lanceException = e;
      }
    }

    if (allocator != null) {
      allocator.close();
    }

    if (arrowException != null) {
      throw new HoodieIOException("Failed to close Arrow reader for Lance file: " + path, arrowException);
    }
    if (lanceException != null) {
      throw new HoodieException("Failed to close Lance reader for file: " + path, lanceException);
    }
  }
}
