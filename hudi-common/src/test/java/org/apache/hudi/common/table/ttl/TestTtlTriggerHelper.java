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

package org.apache.hudi.common.table.ttl;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.TestTimelineUtils;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.ttl.model.TtlTriggerStrategy;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link TtlTriggerHelper}.
 */
public class TestTtlTriggerHelper extends HoodieCommonTestHarness {

  private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

  @ParameterizedTest
  @CsvSource({"COPY_ON_WRITE,commit", "MERGE_ON_READ,deltacommit"})
  public void testTriggerByNumCommits(HoodieTableType type, String action) throws IOException, ParseException {
    // init metaClient
    metaClient = HoodieTestUtils.init(tempDir.toAbsolutePath().toString(), type);
    basePath = metaClient.getBasePath();
    TtlTriggerHelper ttlTriggerHelper = new TtlTriggerHelper(metaClient);
    // empty timeline
    assertFalse(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.NUM_COMMITS, 2));
    // 1 commit in timeline => should NOT trigger
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    String ts = "2023092214354200";
    HoodieInstant instant = new HoodieInstant(true, action, ts);
    activeTimeline.createNewInstant(instant);
    activeTimeline.saveAsComplete(instant, Option.of(TestTimelineUtils.getCommitMetadata(metaClient, basePath, ts, ts, 2, Collections.emptyMap())));
    assertFalse(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.NUM_COMMITS, 2));
    assertTrue(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.NUM_COMMITS, 1));

    // not commit and not replacecommit in timeline  => should NOT trigger
    ts = "2023092215323200";
    instant = new HoodieInstant(true, HoodieTimeline.CLEAN_ACTION, ts);
    activeTimeline.createNewInstant(instant);
    activeTimeline.saveAsComplete(instant, TestTimelineUtils.getCleanMetadata("partition", ts));
    assertFalse(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.NUM_COMMITS, 2));
    assertTrue(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.NUM_COMMITS, 1));

    // 2 commits in timeline => SHOULD trigger
    ts = "2023092215420000";
    instant = new HoodieInstant(true, action, ts);
    activeTimeline.createNewInstant(instant);
    activeTimeline.saveAsComplete(instant, Option.of(TestTimelineUtils.getCommitMetadata(metaClient, basePath, ts, ts, 2, Collections.emptyMap())));
    assertTrue(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.NUM_COMMITS, 2));
    assertFalse(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.NUM_COMMITS, 3));

    // replacecommit but not from delete_partition operation => SHOULD trigger
    ts = "2023092215431111";
    String replacePartition = "2021/01/01";
    String newFilePartition = "2021/01/02";
    instant = new HoodieInstant(true, HoodieTimeline.REPLACE_COMMIT_ACTION, ts);
    activeTimeline.createNewInstant(instant);
    activeTimeline.saveAsComplete(instant,
        Option.of(TestTimelineUtils.getReplaceCommitMetadata(metaClient, basePath, ts, replacePartition, 2,
            newFilePartition, 0, Collections.emptyMap(), WriteOperationType.CLUSTER)));
    assertTrue(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.NUM_COMMITS, 2));
    assertFalse(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.NUM_COMMITS, 3));

    // replacecommit from delete_partition operation => should NOT trigger
    ts = "2023092215461111";
    String replacePartition1 = "2021/01/01";
    String newFilePartition1 = "2021/01/02";
    instant = new HoodieInstant(true, HoodieTimeline.REPLACE_COMMIT_ACTION, ts);
    activeTimeline.createNewInstant(instant);
    activeTimeline.saveAsComplete(instant,
        Option.of(TestTimelineUtils.getReplaceCommitMetadata(metaClient, basePath, ts, replacePartition1, 2,
            newFilePartition1, 0, Collections.emptyMap(), WriteOperationType.DELETE_PARTITION)));
    assertFalse(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.NUM_COMMITS, 2));
    assertFalse(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.NUM_COMMITS, 1));

    // 1 commit after delete_partition replacecommit => should NOT trigger
    ts = "2023092215481111";
    instant = new HoodieInstant(true, action, ts);
    activeTimeline.createNewInstant(instant);
    activeTimeline.saveAsComplete(instant, Option.of(TestTimelineUtils.getCommitMetadata(metaClient, basePath, ts, ts, 2, Collections.emptyMap())));
    assertFalse(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.NUM_COMMITS, 2));
    assertTrue(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.NUM_COMMITS, 1));

    // 2 commits after delete_partition replacecommit => SHOULD trigger
    ts = "2023092215491111";
    instant = new HoodieInstant(true, action, ts);
    activeTimeline.createNewInstant(instant);
    activeTimeline.saveAsComplete(instant, Option.of(TestTimelineUtils.getCommitMetadata(metaClient, basePath, ts, ts, 2, Collections.emptyMap())));
    assertTrue(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.NUM_COMMITS, 2));
    assertTrue(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.NUM_COMMITS, 1));
    assertFalse(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.NUM_COMMITS, 3));
  }

  @Test
  public void testShouldNotTriggerByTimeElapsed() throws IOException, ParseException {
    // init metaClient
    metaClient = HoodieTestUtils.init(tempDir.toAbsolutePath().toString(), HoodieTableType.COPY_ON_WRITE);
    basePath = metaClient.getBasePath();
    TtlTriggerHelper ttlTriggerHelper = new TtlTriggerHelper(metaClient);

    // empty timeline
    assertFalse(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.TIME_ELAPSED, 2));

    // 1 commit less than 2 days before now
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    LocalDateTime now = LocalDateTime.now();
    String ts = HoodieInstantTimeGenerator.getInstantForDateString(now.minusHours(47L).format(formatter));
    HoodieInstant commitInstant = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, ts);
    activeTimeline.createNewInstant(commitInstant);
    activeTimeline.saveAsComplete(commitInstant, Option.of(TestTimelineUtils.getCommitMetadata(metaClient, basePath, ts, ts, 2, Collections.emptyMap())));
    assertFalse(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.TIME_ELAPSED, 2));
    assertTrue(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.TIME_ELAPSED, 1));

    // 1 commit  less than 2 days before now
    ts = HoodieInstantTimeGenerator.getInstantForDateString(now.minusHours(25L).format(formatter));
    commitInstant = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, ts);
    activeTimeline.createNewInstant(commitInstant);
    activeTimeline.saveAsComplete(commitInstant, Option.of(TestTimelineUtils.getCommitMetadata(metaClient, basePath, ts, ts, 2, Collections.emptyMap())));
    assertFalse(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.TIME_ELAPSED, 2));
    assertTrue(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.TIME_ELAPSED, 1));

    // 1 commit more than 2 days before now
    ts = HoodieInstantTimeGenerator.getInstantForDateString(now.minusHours(100L).format(formatter));
    commitInstant = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, ts);
    activeTimeline.createNewInstant(commitInstant);
    activeTimeline.saveAsComplete(commitInstant, Option.of(TestTimelineUtils.getCommitMetadata(metaClient, basePath, ts, ts, 2, Collections.emptyMap())));
    // AND
    // replacecommit older than 2 days
    ts = HoodieInstantTimeGenerator.getInstantForDateString(now.minusHours(72L).format(formatter));
    String replacePartition1 = "2021/01/01";
    String newFilePartition1 = "2021/01/02";
    HoodieInstant replaceInstant1 = new HoodieInstant(true, HoodieTimeline.REPLACE_COMMIT_ACTION, ts);
    activeTimeline.createNewInstant(replaceInstant1);
    activeTimeline.saveAsComplete(replaceInstant1,
        Option.of(TestTimelineUtils.getReplaceCommitMetadata(metaClient, basePath, ts, replacePartition1, 2,
            newFilePartition1, 0, Collections.emptyMap(), WriteOperationType.DELETE_PARTITION)));
    // AND
    // replacecommit younger than 2 days
    ts = HoodieInstantTimeGenerator.getInstantForDateString(now.minusHours(47L).format(formatter));
    replacePartition1 = "2021/01/01";
    newFilePartition1 = "2021/01/02";
    HoodieInstant replaceInstant2 = new HoodieInstant(true, HoodieTimeline.REPLACE_COMMIT_ACTION, ts);
    activeTimeline.createNewInstant(replaceInstant2);
    activeTimeline.saveAsComplete(replaceInstant2,
        Option.of(TestTimelineUtils.getReplaceCommitMetadata(metaClient, basePath, ts, replacePartition1, 2,
            newFilePartition1, 0, Collections.emptyMap(), WriteOperationType.DELETE_PARTITION)));
    assertFalse(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.TIME_ELAPSED, 2));
    assertTrue(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.TIME_ELAPSED, 1));
  }

  @Test
  public void testShouldTriggerByTimeElapsed() throws IOException, ParseException {
    // init metaClient
    metaClient = HoodieTestUtils.init(tempDir.toAbsolutePath().toString(), HoodieTableType.MERGE_ON_READ);
    basePath = metaClient.getBasePath();
    TtlTriggerHelper ttlTriggerHelper = new TtlTriggerHelper(metaClient);

    // TTL: enabled, TIME_ELAPSED = 2 DAYS
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    LocalDateTime now = LocalDateTime.now();
    // 1 deltacommit more than 2 days before now
    String ts = HoodieInstantTimeGenerator.getInstantForDateString(now.minusHours(49L).format(formatter));
    HoodieInstant deltaCommitInstant1 = new HoodieInstant(true, HoodieTimeline.DELTA_COMMIT_ACTION, ts);
    activeTimeline.createNewInstant(deltaCommitInstant1);
    activeTimeline.saveAsComplete(deltaCommitInstant1, Option.of(TestTimelineUtils.getCommitMetadata(metaClient, basePath, ts, ts, 2, Collections.emptyMap())));
    assertTrue(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.TIME_ELAPSED, 2));
    assertTrue(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.TIME_ELAPSED, 1));
    assertFalse(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.TIME_ELAPSED, 3));
    // 1 deltacommit  less than 2 days before now
    ts = HoodieInstantTimeGenerator.getInstantForDateString(now.minusHours(24L).format(formatter));
    HoodieInstant deltaCommitInstant2 = new HoodieInstant(true, HoodieTimeline.DELTA_COMMIT_ACTION, ts);
    activeTimeline.createNewInstant(deltaCommitInstant2);
    activeTimeline.saveAsComplete(deltaCommitInstant2, Option.of(TestTimelineUtils.getCommitMetadata(metaClient, basePath, ts, ts, 2, Collections.emptyMap())));
    assertTrue(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.TIME_ELAPSED, 2));
    assertTrue(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.TIME_ELAPSED, 1));
    assertFalse(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.TIME_ELAPSED, 3));
    // delete commit instants
    activeTimeline.deleteInstantFileIfExists(deltaCommitInstant1);
    activeTimeline.deleteInstantFileIfExists(deltaCommitInstant2);
    assertFalse(activeTimeline.containsInstant(deltaCommitInstant1));
    assertFalse(activeTimeline.containsInstant(deltaCommitInstant2));

    // 1 deltacommit more than 2 days before now
    ts = HoodieInstantTimeGenerator.getInstantForDateString(now.minusHours(50L).format(formatter));
    deltaCommitInstant1 = new HoodieInstant(true, HoodieTimeline.DELTA_COMMIT_ACTION, ts);
    activeTimeline.createNewInstant(deltaCommitInstant1);
    activeTimeline.saveAsComplete(deltaCommitInstant1, Option.of(TestTimelineUtils.getCommitMetadata(metaClient, basePath, ts, ts, 2, Collections.emptyMap())));
    assertTrue(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.TIME_ELAPSED, 2));
    assertTrue(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.TIME_ELAPSED, 1));
    assertFalse(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.TIME_ELAPSED, 3));
    // 1 commit less than 2 days before now
    ts = HoodieInstantTimeGenerator.getInstantForDateString(now.minusHours(47L).format(formatter));
    deltaCommitInstant2 = new HoodieInstant(true, HoodieTimeline.DELTA_COMMIT_ACTION, ts);
    activeTimeline.createNewInstant(deltaCommitInstant2);
    activeTimeline.saveAsComplete(deltaCommitInstant2, Option.of(TestTimelineUtils.getCommitMetadata(metaClient, basePath, ts, ts, 2, Collections.emptyMap())));
    assertTrue(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.TIME_ELAPSED, 2));
    assertTrue(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.TIME_ELAPSED, 1));
    assertFalse(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.TIME_ELAPSED, 3));

    // add replacecommit older than 2 days
    ts = HoodieInstantTimeGenerator.getInstantForDateString(now.minusHours(71L).format(formatter));
    String replacePartition1 = "2021/01/01";
    String newFilePartition1 = "2021/01/02";
    HoodieInstant replaceInstant1 = new HoodieInstant(true, HoodieTimeline.REPLACE_COMMIT_ACTION, ts);
    activeTimeline.createNewInstant(replaceInstant1);
    activeTimeline.saveAsComplete(replaceInstant1,
        Option.of(TestTimelineUtils.getReplaceCommitMetadata(metaClient, basePath, ts, replacePartition1, 2,
            newFilePartition1, 0, Collections.emptyMap(), WriteOperationType.DELETE_PARTITION)));
    assertTrue(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.TIME_ELAPSED, 2));
    assertTrue(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.TIME_ELAPSED, 1));
    assertFalse(ttlTriggerHelper.shouldTriggerNow(TtlTriggerStrategy.TIME_ELAPSED, 3));
  }
}
