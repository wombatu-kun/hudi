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
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.InvalidTtlPolicyException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.type.CollectionType;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Implementation of TtlPolicyDAO with JSON-file as a storage
 */
public class TtlPolicyDAOJson implements TtlPolicyDAO {

  private static final Logger LOG = LoggerFactory.getLogger(TtlPolicyDAOJson.class);

  private static final String TTL_POLICIES_FILE = "ttl_policies.json";

  private final String metaPath;

  private final FileSystem fs;

  private final TtlPolicyComparator comparator;

  public TtlPolicyDAOJson(FileSystem fs, String metaPath, TtlPoliciesConflictResolutionRule resolveConflictsBy) {
    this.fs = fs;
    this.metaPath = metaPath;
    this.comparator = new TtlPolicyComparator(resolveConflictsBy);
  }

  @Override
  public String getPoliciesFileName() {
    return TTL_POLICIES_FILE;
  }

  @Override
  public List<TtlPolicy> getAll() throws IOException {
    return getAllAsMap().values().stream().sorted(comparator).collect(Collectors.toList());
  }

  private Map<String, TtlPolicy> getAllAsMap() throws IOException {
    Map<String, TtlPolicy> policies = new HashMap<>();
    Path policiesPath = new Path(metaPath, getPoliciesFileName());
    if (fs.exists(policiesPath)) {
      try (FSDataInputStream is = fs.open(policiesPath)) {
        parsePolicies(is).forEach(policy -> {
          List<String> violations = policy.validate();
          if (violations.isEmpty()) {
            policies.put(policy.getSpec(), policy);
          } else {
            LOG.error("SKIPPED invalid policy: " + policy + ". Reasons: " + String.join(" ", violations));
          }
        });
      }
    }
    return policies;
  }

  private List<TtlPolicy> parsePolicies(FSDataInputStream is) throws IOException {
    List<TtlPolicy> policies = new ArrayList<>();
    int nBytesToRead = is.available();
    if (nBytesToRead > 0) {
      byte[] bytes = new byte[nBytesToRead];
      is.read(bytes);
      CollectionType listOfPoliciesType = JsonUtils.getObjectMapper().getTypeFactory()
          .constructCollectionType(List.class, TtlPolicy.class);
      policies = JsonUtils.getObjectMapper().readValue(new String(bytes), listOfPoliciesType);
    }
    return policies;
  }

  @Override
  public void save(String jsonStringPolicy) throws IOException {
    if (StringUtils.isNullOrEmpty(jsonStringPolicy)) {
      throw new InvalidTtlPolicyException("JSON-representation of TTL policy must not be empty.");
    }
    try {
      TtlPolicy policy = JsonUtils.getObjectMapper().readValue(jsonStringPolicy, TtlPolicy.class);
      save(policy);
    } catch (JsonProcessingException e) {
      throw new InvalidTtlPolicyException("Unable to parse: " + jsonStringPolicy, e);
    }
  }

  @Override
  public void save(TtlPolicy policy) throws IOException {
    if (policy != null) {
      List<String> violations = policy.validate();
      if (violations.isEmpty()) {
        policy.setUseSince(new Date());
        Map<String, TtlPolicy> policies = getAllAsMap();
        policies.put(policy.getSpec(), policy);
        storePolicies(policies.values());
      } else {
        throw new InvalidTtlPolicyException("Invalid TTL policy: " + String.join(" ", violations));
      }
    } else {
      throw new InvalidTtlPolicyException("TTL policy must not be null");
    }
  }

  @Override
  public void deleteBySpec(String spec) throws IOException {
    if (StringUtils.isNullOrEmpty(spec)) {
      throw new InvalidTtlPolicyException("TTL policy spec must not be empty.");
    }
    Map<String, TtlPolicy> policies = getAllAsMap();
    if (!policies.isEmpty()) {
      if (policies.containsKey(spec)) {
        policies.remove(spec);
        storePolicies(policies.values());
      } else {
        throw new InvalidTtlPolicyException("There is no TTL policy with spec: " + spec);
      }
    } else {
      throw new InvalidTtlPolicyException("The list of TTL policies is empty.");
    }
  }

  @Override
  public void deleteAll() {
    storePolicies(Collections.emptySet());
  }

  private void storePolicies(Collection<TtlPolicy> policies) {
    Path policyPath = new Path(metaPath, getPoliciesFileName());
    FileIOUtils.createFileInPath(fs, policyPath, Option.of(JsonUtils.toString(policies).getBytes()));
  }
}
