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

package org.apache.hudi.source.reader;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.metrics.FlinkStreamReadMetrics;
import org.apache.hudi.source.reader.function.SplitReaderFunction;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.source.split.SerializableComparator;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

/**
 * The split reader of Hoodie source.
 *
 * <p>Each call to {@link #fetch()} returns one bounded minibatch of the currently open split, so a
 * single split spans multiple {@code fetch()} calls. When the open split is exhausted its resources
 * are closed on this (split-fetcher) thread and a finish signal ({@code finishedSplits()}) is
 * returned so Flink's {@code SourceReaderBase} can advance / reach end-of-input. All record reading
 * and resource teardown for a split therefore happen on the same thread, which removes the
 * cross-thread teardown race a live-iterator batch would be exposed to.
 *
 * @param <T> record type
 */
@Slf4j
public class HoodieSourceSplitReader<T> implements SplitReader<HoodieRecordWithPosition<T>, HoodieSourceSplit> {
  // Upper bound on the number of records materialized per fetch() call (one minibatch). Kept as a
  // fixed constant for now (mirrors RecordIterators.DEFAULT_BATCH_SIZE); it can be promoted to a
  // Flink option later if a tunable per-fetch bound is ever needed.
  private static final int DEFAULT_MINI_BATCH_SIZE = 2048;

  private final SerializableComparator<HoodieSourceSplit> splitComparator;
  private final Queue<HoodieSourceSplit> splits;
  private final FlinkStreamReadMetrics readerMetrics;
  private final SplitReaderFunction<T> readerFunction;
  private final Option<RecordLimiter> recordLimiter;
  private transient HoodieSourceSplit currentSplit;

  public HoodieSourceSplitReader(
      String tableName,
      SourceReaderContext context,
      SplitReaderFunction<T> readerFunction,
      SerializableComparator<HoodieSourceSplit> splitComparator,
      Option<RecordLimiter> recordLimiter) {
    this.splitComparator = splitComparator;
    this.splits = new ArrayDeque<>();
    this.readerFunction = readerFunction;
    this.recordLimiter = recordLimiter;
    this.readerMetrics = new FlinkStreamReadMetrics(context.metricGroup(), tableName);
    this.readerMetrics.registerMetrics();
  }

  @Override
  public RecordsWithSplitIds<HoodieRecordWithPosition<T>> fetch() throws IOException {
    if (currentSplit == null) {
      // Limit already satisfied: drain any remaining locally-queued splits as immediately finished
      // so that Flink's SourceReaderBase can reach end-of-input cleanly.
      if (recordLimiter.map(RecordLimiter::isLimitReached).orElse(false)) {
        return drainRemainingAsSplitsFinished();
      }
      HoodieSourceSplit nextSplit = splits.poll();
      if (nextSplit == null) {
        // return an empty result, which will lead to split fetch to be idle.
        // SplitFetcherManager will then close idle fetcher.
        return new RecordsBySplits<>(Collections.emptyMap(), Collections.emptySet());
      }
      currentSplit = nextSplit;
      readerFunction.open(currentSplit);
    }

    // Read the next bounded minibatch of the open split, unless the global limit is already reached.
    if (!recordLimiter.map(RecordLimiter::isLimitReached).orElse(false)) {
      BatchRecords<T> batch = readerFunction.readBatch(currentSplit, DEFAULT_MINI_BATCH_SIZE);
      if (batch != null) {
        return recordLimiter.map(rl -> rl.wrap(batch)).orElse(batch);
      }
    }

    // Split exhausted (or the limit was reached mid-split): close its resources on this
    // (split-fetcher) thread first, then emit the finish signal so SourceReaderBase can advance.
    readerFunction.closeCurrentSplit();
    return finishSplit();
  }

  @Override
  public void handleSplitsChanges(SplitsChange<HoodieSourceSplit> splitsChange) {
    if (!(splitsChange instanceof SplitsAddition)) {
      throw new UnsupportedOperationException(
          String.format("Unsupported split change: %s", splitsChange.getClass()));
    }

    if (splitComparator != null) {
      List<HoodieSourceSplit> newSplits = new ArrayList<>(splitsChange.splits());
      newSplits.sort(splitComparator);
      log.info("Add {} splits to reader: {}", newSplits.size(), newSplits);
      splits.addAll(newSplits);
    } else {
      log.info("Add {} splits to reader", splitsChange.splits().size());
      splits.addAll(splitsChange.splits());
    }
  }

  @Override
  public void wakeUp() {
    // Nothing to do
  }

  /**
   * SourceSplitReader only reads splits sequentially. When waiting for watermark alignment
   * the SourceOperator will stop processing and recycling the fetched batches. Based on this the
   * {@code pauseOrResumeSplits} and the {@code wakeUp} are left empty.
   * @param splitsToPause splits to pause
   * @param splitsToResume splits to resume
   */
  @Override
  public void pauseOrResumeSplits(
      Collection<HoodieSourceSplit> splitsToPause,
      Collection<HoodieSourceSplit> splitsToResume) {
  }

  @Override
  public void close() throws Exception {
    readerFunction.close();
  }

  private RecordsWithSplitIds<HoodieRecordWithPosition<T>> finishSplit() {
    RecordsWithSplitIds<HoodieRecordWithPosition<T>> records = BatchRecords.lastBatchRecords(currentSplit.splitId());
    currentSplit = null;
    return records;
  }

  /**
   * Returns a batch that immediately marks all locally-queued splits as finished, allowing
   * Flink's SourceReaderBase to reach end-of-input without reading any more records.
   */
  private RecordsWithSplitIds<HoodieRecordWithPosition<T>> drainRemainingAsSplitsFinished() {
    Set<String> finishedIds = new HashSet<>();
    HoodieSourceSplit split;
    while ((split = splits.poll()) != null) {
      finishedIds.add(split.splitId());
    }
    return new RecordsBySplits<>(Collections.emptyMap(), finishedIds);
  }
}
