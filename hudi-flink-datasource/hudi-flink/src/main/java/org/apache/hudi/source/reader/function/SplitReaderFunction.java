/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.source.reader.function;

import org.apache.hudi.source.reader.BatchRecords;
import org.apache.hudi.source.split.HoodieSourceSplit;

import java.io.Serializable;

/**
 * Interface for a split read function.
 *
 * <p>A reader function is a stateful, per-split cursor driven entirely on the Flink split-fetcher
 * thread by {@link org.apache.hudi.source.reader.HoodieSourceSplitReader#fetch()}:
 * {@link #open(HoodieSourceSplit)} creates the record iterator and its underlying I/O resources for
 * a split, {@link #readBatch(HoodieSourceSplit, int)} drains the next bounded minibatch, and
 * {@link #closeCurrentSplit()} releases the split's resources once it is exhausted. Because open,
 * read and close all run on the same thread, no record or I/O resource is ever touched concurrently.
 *
 * @param <T> record type
 */
public interface SplitReaderFunction<T> extends Serializable {

  /**
   * Opens {@code split} for reading: creates the record iterator and its underlying I/O resources,
   * and skips the records already consumed ({@link HoodieSourceSplit#getConsumed()}) so a recovered
   * split resumes at the right position.
   */
  void open(HoodieSourceSplit split);

  /**
   * Drains up to {@code batchSize} records from the currently open split into a materialized
   * {@link BatchRecords} minibatch. Returns {@code null} once the split is exhausted.
   */
  BatchRecords<T> readBatch(HoodieSourceSplit split, int batchSize);

  /**
   * Closes the currently open split's iterator and I/O resources. Called when the split is
   * exhausted, a read fails, or the read is stopped early. Safe to call when no split is open.
   */
  void closeCurrentSplit();

  /**
   * Closes the reader function entirely (idempotent). Invoked by
   * {@link org.apache.hudi.source.reader.HoodieSourceSplitReader#close()} on the split-fetcher
   * thread.
   */
  void close() throws Exception;
}
