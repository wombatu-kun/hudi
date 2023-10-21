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

import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.ttl.model.TtlPoliciesConflictResolutionRule;
import org.apache.hudi.common.table.ttl.model.TtlTriggerStrategy;
import org.apache.hudi.exception.InvalidTtlPolicyException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Utility helper class to work with TTL configuration parameters
 */
public class TtlConfigurer {

  private TtlConfigurer() {
  }

  public static Properties setAndGetTtlConf(FileSystem fs, String metadataFolder,
                                            String enabled, String strategy, String value, String resolveConflictsBy) {
    Properties updatedProps = new Properties();
    if (enabled != null) {
      updatedProps.setProperty(HoodieTableConfig.TTL_POLICIES_ENABLED.key(), Boolean.valueOf(enabled).toString());
    }
    if (strategy != null) {
      try {
        updatedProps.setProperty(HoodieTableConfig.TTL_TRIGGER_STRATEGY.key(), TtlTriggerStrategy.valueOf(strategy).toString());
      } catch (IllegalArgumentException e) {
        throw new InvalidTtlPolicyException("Strategy " + strategy + " is invalid. Valid TTL trigger strategies are: "
            + Arrays.stream(TtlTriggerStrategy.values()).map(Enum::name).collect(Collectors.joining(", ")));
      }
    }
    if (value != null) {
      int newVal;
      RuntimeException ex = new InvalidTtlPolicyException("Trigger value must be integer greater than 0, but you tried to set " + value);
      try {
        newVal = Integer.parseInt(value);
        if (newVal <= 0) {
          throw ex;
        }
      } catch (NumberFormatException e) {
        throw ex;
      }
      updatedProps.setProperty(HoodieTableConfig.TTL_TRIGGER_VALUE.key(), String.valueOf(newVal));
    }
    if (resolveConflictsBy != null) {
      try {
        updatedProps.setProperty(HoodieTableConfig.TTL_POLICIES_CONFLICT_RESOLUTION_RULE.key(),
            TtlPoliciesConflictResolutionRule.valueOf(resolveConflictsBy).toString());
      } catch (IllegalArgumentException e) {
        throw new InvalidTtlPolicyException("TTL policies conflict resolution rule " + resolveConflictsBy + " is invalid."
            + " Valid TTL policies conflict resolution rules are: "
            + Arrays.stream(TtlPoliciesConflictResolutionRule.values()).map(Enum::name).collect(Collectors.joining(", ")));
      }
    }
    if (!updatedProps.isEmpty()) {
      HoodieTableConfig.update(fs, new Path(metadataFolder), updatedProps);
    }
    return updatedProps;
  }
}
