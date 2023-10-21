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

import org.apache.hudi.common.table.ttl.model.TtlPoliciesConflictResolutionRule;
import org.apache.hudi.common.table.ttl.model.TtlPolicy;

import java.util.Comparator;

/**
 * Defines natural ordering for TTL policies defined by
 * TtlPoliciesConflictResolutionRule in HoodieTableConfig
 */
public class TtlPolicyComparator implements Comparator<TtlPolicy> {

  private final TtlPoliciesConflictResolutionRule resolveConflictsBy;

  public TtlPolicyComparator(TtlPoliciesConflictResolutionRule resolveConflictsBy) {
    this.resolveConflictsBy = resolveConflictsBy;
  }

  @Override
  public int compare(TtlPolicy t1, TtlPolicy t2) {
    int level1 = t1.getLevel().ordinal();
    int level2 = t2.getLevel().ordinal();
    if (level1 == level2) {
      return TtlPoliciesConflictResolutionRule.MIN_TTL.equals(resolveConflictsBy)
          ? t1.convertTtlToDays() - t2.convertTtlToDays()
          : t2.convertTtlToDays() - t1.convertTtlToDays();
    } else {
      return Integer.compare(level1, level2);
    }
  }
}
