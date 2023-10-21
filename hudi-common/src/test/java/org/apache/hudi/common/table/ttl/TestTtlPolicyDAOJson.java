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
import org.apache.hudi.common.table.ttl.model.TtlPolicyLevel;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.InvalidTtlPolicyException;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link TtlPolicyDAOJson}.
 */
public class TestTtlPolicyDAOJson extends HoodieCommonTestHarness {

  private TtlPolicyDAO ttlPolicyDAO;

  @BeforeEach
  public void setUp() throws Exception {
    initMetaClient();
    this.ttlPolicyDAO = new TtlPolicyDAOJson(metaClient.getFs(), metaClient.getMetaPath(), TtlPoliciesConflictResolutionRule.MAX_TTL);
  }

  @Test
  public void testTtlPolicyCRUDSuccess() throws IOException {
    // policies file name
    assertEquals("ttl_policies.json", ttlPolicyDAO.getPoliciesFileName());

    // no policies
    List<TtlPolicy> policies = ttlPolicyDAO.getAll();
    assertNotNull(policies);
    assertEquals(0, policies.size());

    // add and get 1 policy
    TtlPolicy policy1 = new TtlPolicy("*", TtlPolicyLevel.PARTITION, 12, ChronoUnit.MONTHS);
    String json = JsonUtils.toString(policy1);
    System.out.println("1 ttl policy json: " + json);
    ttlPolicyDAO.save(json);
    policies = ttlPolicyDAO.getAll();
    assertEquals(1, policies.size());
    assertThrows(InvalidTtlPolicyException.class, () -> ttlPolicyDAO.deleteBySpec("2023/*"), "There is no policy with spec: 2023/*");

    // add and get 2 policies
    TtlPolicy policy2 = new TtlPolicy("2023", TtlPolicyLevel.PARTITION, 20, ChronoUnit.YEARS);
    json = JsonUtils.toString(policy2);
    System.out.println("2 ttl policy json: " + json);
    ttlPolicyDAO.save(json);
    policies = ttlPolicyDAO.getAll();
    assertEquals(2, policies.size());
    policy1 = policies.get(1);
    policy2 = policies.get(0);
    assertEquals(12, policy1.getValue());
    assertEquals(20, policy2.getValue());
    assertEquals(ChronoUnit.YEARS, policy2.getUnits());

    // update and get 2 policies
    policy1.setLevel(TtlPolicyLevel.RECORD);
    json = JsonUtils.toString(policy1);
    ttlPolicyDAO.save(json);
    policies = ttlPolicyDAO.getAll();
    assertEquals(2, policies.size());
    TtlPolicy policyUpdated = policies.get(1);
    assertEquals(TtlPolicyLevel.RECORD, policyUpdated.getLevel());
    assertTrue(policy1.getUseSince().getTime() < policyUpdated.getUseSince().getTime());

    // delete by spec
    ttlPolicyDAO.deleteBySpec(policy1.getSpec());
    policies = ttlPolicyDAO.getAll();
    assertEquals(1, policies.size());
    assertEquals(policy2.getSpec(), policies.get(0).getSpec());

    // delete all
    ttlPolicyDAO.deleteAll();
    policies = ttlPolicyDAO.getAll();
    assertEquals(0, policies.size());
    Option<byte[]> fileContent = FileIOUtils.readDataFromPath(metaClient.getFs(),
        new Path(metaClient.getMetaPath() + Path.SEPARATOR + ttlPolicyDAO.getPoliciesFileName()));
    assertTrue(fileContent.isPresent());
    assertEquals("[]", new String(fileContent.get()));
    assertDoesNotThrow(() -> ttlPolicyDAO.deleteAll());
  }

  @Test
  public void testPolicyCRUDFailures() {
    // delete: failures on empty
    assertThrows(InvalidTtlPolicyException.class, () -> ttlPolicyDAO.deleteBySpec(null), "TTL policy spec must not be empty.");
    assertThrows(InvalidTtlPolicyException.class, () -> ttlPolicyDAO.deleteBySpec(""), "TTL policy spec must not be empty.");
    assertThrows(InvalidTtlPolicyException.class, () -> ttlPolicyDAO.deleteBySpec("ololo"), "The list of policies is already empty.");
    // save: failures on empty arg
    assertThrows(InvalidTtlPolicyException.class, () -> ttlPolicyDAO.save((String)null), "JSON-representation of TTL policy must not be empty.");
    assertThrows(InvalidTtlPolicyException.class, () -> ttlPolicyDAO.save(""), "JSON-representation of TTL policy must not be empty.");

    // save: failures on invalid json
    String json0 = "{\"spec\":null,\"level\":null,\"value\":null,\"units\":null";
    System.out.println("1 ttl policy json: " + json0);
    assertThrows(InvalidTtlPolicyException.class, () -> ttlPolicyDAO.save(json0), "Unable to parse: " + json0);

    // save: failures on invalid fields
    TtlPolicy policy1 = new TtlPolicy();
    policy1.setUnits(ChronoUnit.HOURS);
    String json1 = JsonUtils.toString(policy1);
    System.out.println("1 ttl policy json: " + json1);
    assertThrows(InvalidTtlPolicyException.class, () -> ttlPolicyDAO.save(json1),
        "Invalid TTL policy: 'spec' must not be empty; 'level' must not be null; 'value' must be integer greater than 0;"
            + " 'units' must be one of types: YEARS, MONTHS, WEEKS, DAYS;");

    TtlPolicy policy2 = new TtlPolicy("*", null, 0, null);
    String json2 = JsonUtils.toString(policy2);
    System.out.println("2 ttl policy json: " + json2);
    assertThrows(InvalidTtlPolicyException.class, () -> ttlPolicyDAO.save(json2),
        "Invalid TTL policy: 'level' must not be null; 'value' must be integer greater than 0; 'units' must not be null;");
  }

  @Test
  public void testTtlPolicySpecValidation() throws IOException {
    // slashed only => invalid
    TtlPolicy policy = new TtlPolicy("//", TtlPolicyLevel.PARTITION, 2, ChronoUnit.DAYS);
    String json1 = JsonUtils.toString(policy);
    System.out.println("ttl policy json: " + json1);
    assertThrows(InvalidTtlPolicyException.class, () -> ttlPolicyDAO.save(json1),
        "Invalid TTL policy: 'spec' must not consist of /;");

    // cut ending /
    policy = new TtlPolicy("*/", TtlPolicyLevel.PARTITION, 2, ChronoUnit.DAYS);
    ttlPolicyDAO.save(policy);
    List<TtlPolicy> policies = ttlPolicyDAO.getAll();
    assertEquals(1, policies.size());
    assertEquals("*", policies.get(0).getSpec());
    ttlPolicyDAO.deleteAll();

    // cut starting /
    policy = new TtlPolicy("/*/2020", TtlPolicyLevel.PARTITION, 2, ChronoUnit.DAYS);
    ttlPolicyDAO.save(policy);
    policies = ttlPolicyDAO.getAll();
    assertEquals(1, policies.size());
    assertEquals("*/2020", policies.get(0).getSpec());
    ttlPolicyDAO.deleteAll();

    // cut starting and ending /
    policy = new TtlPolicy("/qq/*/", TtlPolicyLevel.PARTITION, 2, ChronoUnit.DAYS);
    ttlPolicyDAO.save(policy);
    policies = ttlPolicyDAO.getAll();
    assertEquals(1, policies.size());
    assertEquals("qq/*", policies.get(0).getSpec());
    ttlPolicyDAO.deleteAll();

    // common valid, nothing to cut
    policy = new TtlPolicy("*", TtlPolicyLevel.PARTITION, 2, ChronoUnit.DAYS);
    ttlPolicyDAO.save(policy);
    policies = ttlPolicyDAO.getAll();
    assertEquals(1, policies.size());
    assertEquals("*", policies.get(0).getSpec());
  }

  @Test
  public void testPolicyOrdering() throws IOException {
    ttlPolicyDAO.save(JsonUtils.toString(new TtlPolicy("*", TtlPolicyLevel.PARTITION, 12, ChronoUnit.MONTHS)));
    ttlPolicyDAO.save(JsonUtils.toString(new TtlPolicy("01", TtlPolicyLevel.PARTITION, 2, ChronoUnit.MONTHS)));
    ttlPolicyDAO.save(JsonUtils.toString(new TtlPolicy("00", TtlPolicyLevel.RECORD, 6, ChronoUnit.MONTHS)));
    ttlPolicyDAO.save(JsonUtils.toString(new TtlPolicy("02", TtlPolicyLevel.PARTITION, 2, ChronoUnit.YEARS)));

    List<TtlPolicy> policyList = ttlPolicyDAO.getAll();
    System.out.println("MAX_TTL rule: ");
    policyList.forEach(System.out::println);
    assertEquals("02", policyList.get(0).getSpec());
    assertEquals("*", policyList.get(1).getSpec());
    assertEquals("01", policyList.get(2).getSpec());
    assertEquals("00", policyList.get(3).getSpec());

    ttlPolicyDAO = new TtlPolicyDAOJson(metaClient.getFs(), metaClient.getMetaPath(), TtlPoliciesConflictResolutionRule.MIN_TTL);
    policyList = ttlPolicyDAO.getAll();
    System.out.println("MIN_TTL rule: ");
    policyList.forEach(System.out::println);
    assertEquals("01", policyList.get(0).getSpec());
    assertEquals("*", policyList.get(1).getSpec());
    assertEquals("02", policyList.get(2).getSpec());
    assertEquals("00", policyList.get(3).getSpec());
  }

}
