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

import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.ttl.model.TtlTriggerStrategy;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.InvalidTtlPolicyException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

/**
 * Helper class to decide should TTL processing trigger or not.
 */
public class TtlTriggerHelper {

  private static final Logger LOG = LoggerFactory.getLogger(TtlTriggerHelper.class);

  private final HoodieTableMetaClient metaClient;

  public TtlTriggerHelper(HoodieTableMetaClient metaClient) {
    this.metaClient = metaClient;
  }

  public boolean shouldTriggerNow(TtlTriggerStrategy ttlTrigger, int triggerValue) throws ParseException {
    LOG.info("TTL trigger helper tests conditions: " + ttlTrigger + "=" + triggerValue);
    metaClient.reloadActiveTimeline();
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    switch (ttlTrigger) {
      case NUM_COMMITS:  return shouldTriggerByNumCommits(timeline, triggerValue);
      case TIME_ELAPSED: return shouldTriggerByTimeElapsed(timeline, triggerValue);
      default: throw new InvalidTtlPolicyException("Unsupported TtlTriggerStrategy: " + ttlTrigger);
    }
  }

  private boolean shouldTriggerByNumCommits(HoodieActiveTimeline timeline, int triggerValue) {
    int commitCount = 0;
    List<HoodieInstant> instants = timeline.getInstants();
    if (instants.size() == 0) {
      LOG.debug("Active timeline is empty");
      return false;
    } else {
      LOG.debug("Current size of active timeline is: " + instants.size());
    }
    for (int k = instants.size() - 1; k >= 0; k--) {
      HoodieInstant instant = instants.get(k);
      String filename = instant.getFileName();
      switch (instant.getAction()) {
        case HoodieTimeline.REPLACE_COMMIT_ACTION:
          if (isReplaceCommitOfDeletePartitionOperation(timeline, instant)) {
            return false;
          }
          break;
        case HoodieTimeline.DELTA_COMMIT_ACTION:
        case HoodieTimeline.COMMIT_ACTION:
          if (instant.isCompleted()) {
            commitCount++;
            LOG.debug("Instant " + filename + " is completed commit/deltacommit, commitCount is " + commitCount);
            if (commitCount >= triggerValue) {
              LOG.debug("commitCount = " + triggerValue + ". TTL policy service should trigger now!");
              return true;
            }
          } else {
            LOG.debug("SKIP incomplete instant: " + filename);
          }
          break;
        default: // other actions are not interesting
          LOG.debug("SKIP instant: " + filename);
      }
    }
    return false;
  }

  private boolean shouldTriggerByTimeElapsed(HoodieActiveTimeline timeline, int triggerValue) throws ParseException {
    List<HoodieInstant> instants = timeline.getCompletedReplaceTimeline().getInstants();
    LOG.debug("Current number of replacecommits in active timeline is: " + instants.size());
    HoodieInstant replaceCommitInstant = null;
    for (int k = instants.size() - 1; k >= 0; k--) {
      HoodieInstant instant = instants.get(k);
      if (isReplaceCommitOfDeletePartitionOperation(timeline, instant)) {
        replaceCommitInstant = instant;
        break;
      }
    }

    Date date = null;
    if (replaceCommitInstant != null) {
      date = HoodieInstantTimeGenerator.parseDateFromInstantTime(replaceCommitInstant.getTimestamp());
    } else {
      Option<HoodieInstant> firstCommitInstant = timeline.getTimelineOfActions(
          new HashSet<>(Arrays.asList(HoodieTimeline.COMMIT_ACTION, HoodieTimeline.DELTA_COMMIT_ACTION))).firstInstant();
      if (firstCommitInstant.isPresent()) {
        date = HoodieInstantTimeGenerator.parseDateFromInstantTime(firstCommitInstant.get().getTimestamp());
      }
    }
    if (date != null) {
      return TtlPolicyUtils.timeIsUp(date, triggerValue, new Date());
    } else {
      return false;
    }
  }

  private boolean isReplaceCommitOfDeletePartitionOperation(HoodieTimeline timeline, HoodieInstant instant) {
    boolean result = false;
    String filename = instant.getFileName();
    LOG.debug("Instant is a replacecommit, read: " + filename);
    HoodieReplaceCommitMetadata replaceCommitMetadata = timeline.getInstantDetails(instant)
        .map(bytes -> {
          try {
            return HoodieReplaceCommitMetadata.fromBytes(bytes, HoodieReplaceCommitMetadata.class);
          } catch (IOException e) {
            return null;
          }
        }).orElse(null);
    if (replaceCommitMetadata == null) {
      LOG.warn("Failed to read replacecommit metadata file: " + filename);
    } else if (WriteOperationType.DELETE_PARTITION.equals(replaceCommitMetadata.getOperationType())) {
      LOG.debug("Replacecommit instant " + filename + " was created by delete_partition");
      result = true;
    } else {
      LOG.debug("Replacecommit instant " + filename + " was not from delete_partition operation, SKIP");
    }
    return result;
  }
}
