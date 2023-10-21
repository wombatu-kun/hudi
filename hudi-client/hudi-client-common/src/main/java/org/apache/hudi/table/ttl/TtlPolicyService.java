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

package org.apache.hudi.table.ttl;

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.table.ttl.TtlPolicyDAO;
import org.apache.hudi.common.table.ttl.TtlPolicyUtils;
import org.apache.hudi.common.table.ttl.TtlTriggerHelper;
import org.apache.hudi.common.table.ttl.model.TtlPolicy;
import org.apache.hudi.common.table.ttl.model.TtlTriggerStrategy;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.InvalidTtlPolicyException;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;

/**
 * Main service class for processing TTL policies
 */
public class TtlPolicyService {

  private static final Logger LOG = LoggerFactory.getLogger(TtlPolicyService.class);
  private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
  public static final int DELETION_SAMPLE_SIZE = 23;
  public static final int UPDATE_PARTITION_METADATA_TASK_ID = 23;

  private final HoodieTableMetaClient metaClient;
  private final TtlPolicyDAO ttlPolicyDAO;
  private final HoodieTableConfig tableConfig;
  private final HoodieEngineContext engineContext;
  private final TtlTriggerHelper ttlTriggerHelper;
  private final HoodieTable hoodieTable;

  public TtlPolicyService(HoodieTableMetaClient metaClient, HoodieEngineContext engineContext, HoodieTable hoodieTable) {
    this.metaClient = metaClient;
    this.ttlPolicyDAO = metaClient.getTtlPolicyDAO();
    this.tableConfig = new HoodieTableConfig(metaClient.getFs(), metaClient.getMetaPath(), null, null);
    this.engineContext = engineContext;
    this.ttlTriggerHelper = new TtlTriggerHelper(metaClient);
    this.hoodieTable = hoodieTable;
  }

  // Run TTL-processing manually by user command
  // TTL configuration from HoodieTableConfig is not taken into account
  public List<TtlMatch> run(boolean dryRun) throws IOException, ParseException {
    LOG.info("TTL policy service started " + (dryRun ? "DRY-RUN" : "RUN") + " at " + new Date());

    List<TtlPolicy> partitionLevelPolicies = new ArrayList<>();
    List<TtlPolicy> recordLevelPolicies = new ArrayList<>();
    getPolicies(partitionLevelPolicies, recordLevelPolicies);

    List<TtlMatch> expiredPartitions = findExpiredPartitions(partitionLevelPolicies, 0);
    if (!dryRun) {
      deleteExpiredPartitions(expiredPartitions);
    }
    return expiredPartitions;
  }

  // Run TTL-processing inline automatically after each commit (as other table services)
  // but TTL configuration from HoodieTableConfig is always taken into account
  public void runInline() throws ParseException, IOException {
    boolean ttlEnabled = tableConfig.getBooleanOrDefault(HoodieTableConfig.TTL_POLICIES_ENABLED);
    if (ttlEnabled) {
      TtlTriggerStrategy ttlTrigger = TtlTriggerStrategy.valueOf(tableConfig.getStringOrDefault(HoodieTableConfig.TTL_TRIGGER_STRATEGY));
      int triggerValue = Integer.parseInt(tableConfig.getStringOrDefault(HoodieTableConfig.TTL_TRIGGER_VALUE));
      if (ttlTriggerHelper.shouldTriggerNow(ttlTrigger, triggerValue)) {
        run(false);
      }
    }
  }

  private void deleteExpiredPartitions(List<TtlMatch> expiredPartitions) {
    if (!expiredPartitions.isEmpty()) {
      List<String> paths = expiredPartitions.stream().map(TtlMatch::getSource).collect(Collectors.toList());
      String instantTime = HoodieInstantTimeGenerator.getInstantForDateString(LocalDateTime.now().format(FORMATTER));
      hoodieTable.getConfig().setValue(HoodieWriteConfig.AUTO_COMMIT_ENABLE, "true");
      hoodieTable.deletePartitions(engineContext, instantTime, paths);
    }
    LOG.info("TTL policy service finished for PARTITION level at " + new Date());
  }

  /**
   * Searches expired partitions by list of TTL policies
   * @param policies list of TTL policies ordered accordingly to TtlPoliciesConflictResolutionRule
   * @param limit upper limit of expired partitions you want to get or 0 - to get them all
   * @return list of expired partitions and corresponding TTL policy info
   * @throws ParseException
   */
  public List<TtlMatch> findExpiredPartitions(List<TtlPolicy> policies, int limit) throws ParseException {
    List<TtlMatch> result = new ArrayList<>();
    if (policies.isEmpty()) {
      LOG.info("TTL policy service: partition level policies not found");
      return  result;
    }

    List<String> partitions = FSUtils.getAllPartitionPaths(engineContext, metaClient.getBasePathV2().toString(),
        HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS);
    // exclude all partitions that should not be deleted
    partitions.removeAll(findExclusions());

    Map<String, Pattern> partitionPatterns = TtlPolicyUtils.compilePartitionPatterns(policies);
    Date now = new Date();
    if (limit > 0) {
      // reverse list to show the freshest partitions in deletion sample
      partitions = partitions.stream().sorted().collect(Collectors.toList());
      Collections.reverse(partitions);
    }
    for (String partition: partitions) {
      for (TtlPolicy policy: policies) {
        if (TtlPolicyUtils.doesPartitionMatchPattern(partition, partitionPatterns.get(policy.getSpec()))) {
          doesPartitionMatchTtl(policy, partition, now).ifPresent(result::add);
          break;
        }
      }
      if (limit > 0 && result.size() == limit) {
        break;
      }
    }
    LOG.info("TTL policy service collected expired partitions at " + new Date());
    return result;
  }

  private void getPolicies(List<TtlPolicy> partitionPolicies, List<TtlPolicy> recordPolicies) throws IOException {
    List<TtlPolicy> allPolicies = ttlPolicyDAO.getAll();
    if (allPolicies.isEmpty()) {
      throw new InvalidTtlPolicyException("TTL policies are not configured");
    } else {
      allPolicies.forEach(p -> {
        switch (p.getLevel()) {
          case PARTITION:
            partitionPolicies.add(p);
            break;
          case RECORD:
            recordPolicies.add(p);
            break;
          default:
            throw new InvalidTtlPolicyException("Unexpected TTL policy level: " + p.getLevel());
        }
      });
    }
  }

  private Set<String> findExclusions() {
    // partitions affected by savepoints from activetimeline should not be deleted
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    Set<String> exclusions = new HashSet<>(TimelineUtils.getAffectedPartitions(activeTimeline.getSavePointTimeline()));
    // partitions affected by savepoints from archivedtimeline should not be deleted
    HoodieArchivedTimeline archivedTimeline = metaClient.getArchivedTimeline();
    exclusions.addAll(TimelineUtils.getAffectedPartitions(archivedTimeline.getSavePointTimeline()));
    // partitions affected by pending clustering should not be deleted
    ClusteringUtils.getAllPendingClusteringPlans(metaClient)
        .flatMap(pair -> pair.getRight().getInputGroups().stream())
        .flatMap(inputGroup -> inputGroup.getSlices().stream())
        .forEach(slice -> exclusions.add(slice.getPartitionPath()));
    // partitions affected by pending compaction should not be deleted
    activeTimeline.filterPendingCompactionTimeline().getInstants().stream()
        .map(instant -> {
          try {
            return Option.of(CompactionUtils.getCompactionPlan(metaClient, instant.getTimestamp()));
          } catch (Exception e) {
            return Option.ofNullable((HoodieCompactionPlan) null);
          }
        })
        .filter(Option::isPresent)
        .flatMap(plan -> plan.get().getOperations().stream())
        .forEach(operation -> exclusions.add(operation.getPartitionPath()));
    return exclusions;
  }

  private Option<TtlMatch> doesPartitionMatchTtl(TtlPolicy policy, String partition, Date date) throws ParseException {
    HoodiePartitionMetadata partitionMetadata = new HoodiePartitionMetadata(metaClient.getFs(), new Path(metaClient.getBasePathV2(), partition));
    Option<String> lastUpdateOpt = partitionMetadata.readPartitionUpdatedCommitTime();
    if (lastUpdateOpt.isPresent()) {
      Date lastUpdate = HoodieInstantTimeGenerator.parseDateFromInstantTime(lastUpdateOpt.get());
      if (TtlPolicyUtils.timeIsUp(lastUpdate, policy.convertTtlToDays(), date)) {
        TtlMatch partitionMatch = new TtlMatch(partition, lastUpdateOpt.get(),
            policy.getSpec(), policy.getLevel(), policy.getValue(), policy.getUnits());
        LOG.info("Partition matches TTL policy: " + partitionMatch);
        return Option.of(partitionMatch);
      }
    } else {
      LOG.debug("TTL policy service SKIPS " + partition + ": lastUpdateTime is unknown");
    }
    return Option.empty();
  }

  public Map<String, String> updatePartitionsMetadata(boolean dryRun) {
    Map<String, String> updatedPartitions = new HashMap<>();
    updatePartitionsMetadataByTimeline(metaClient.getActiveTimeline(), updatedPartitions, dryRun);
    updatePartitionsMetadataByTimeline(metaClient.getArchivedTimeline(), updatedPartitions, dryRun);
    return updatedPartitions;
  }

  private void updatePartitionsMetadataByTimeline(HoodieDefaultTimeline timeline, Map<String, String> updatedPartitions, boolean dryRun) {
    // iterate over timeline with only completed commits/deltacommits-instants
    timeline.getTimelineOfActions(CollectionUtils.createSet(COMMIT_ACTION, DELTA_COMMIT_ACTION))
        .filterCompletedInstants()
        // from now to past
        .getReverseOrderedInstants()
        .forEach(commit -> {
          try {
            HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(commit).get(), HoodieCommitMetadata.class);
            commitMetadata.getPartitionToWriteStats().keySet().stream() // iterate over partitions affected by this commit/deltacommit
                .filter(partition -> {
                  if (!updatedPartitions.containsKey(partition)) { // only partitions that have not been processed yet
                    try { // check if partition exists in filesystem
                      boolean exists =  metaClient.getFs().exists(new Path(metaClient.getBasePathV2(), partition));
                      if (!exists) {
                        updatedPartitions.put(partition, "does not exist");
                      }
                      return exists;
                    } catch (IOException e) {
                      LOG.error("Failed to check existence of partition: " + partition);
                      // add partition to processed
                      updatedPartitions.put(partition, e.getMessage());
                      return false;
                    }
                  } else {
                    return false;
                  }
                })
                .forEach(partition -> {
                  HoodiePartitionMetadata partitionMetadata = new HoodiePartitionMetadata(metaClient.getFs(), new Path(metaClient.getBasePathV2(), partition));
                  Option<String> lastUpdateTime = partitionMetadata.readPartitionUpdatedCommitTime();
                  if (!lastUpdateTime.isPresent()) { // only partitions without lastUpdateTime
                    if (!dryRun) {
                      String createdTime = partitionMetadata.readPartitionCreatedCommitTime().get();
                      partitionMetadata = new HoodiePartitionMetadata(metaClient.getFs(), createdTime, commit.getTimestamp(),
                          metaClient.getBasePathV2(), new Path(metaClient.getBasePathV2(), partition),
                          metaClient.getTableConfig().getPartitionMetafileFormat());
                      partitionMetadata.trySave(UPDATE_PARTITION_METADATA_TASK_ID);
                    }
                    updatedPartitions.put(partition, commit.getTimestamp());
                  } else {
                    updatedPartitions.put(partition, "already updated:" + lastUpdateTime.get());
                  }
                });
          } catch (IOException e) {
            LOG.warn("Failed to get partitions written at " + commit.getTimestamp());
          }
        });
  }
}
