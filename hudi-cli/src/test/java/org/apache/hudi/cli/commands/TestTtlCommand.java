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

package org.apache.hudi.cli.commands;

import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.functional.CLIFunctionalTestHarness;
import org.apache.hudi.cli.testutils.ShellEvaluationResultUtil;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.ttl.TtlPolicyDAO;
import org.apache.hudi.common.table.ttl.model.TtlPoliciesConflictResolutionRule;
import org.apache.hudi.common.table.ttl.model.TtlPolicy;
import org.apache.hudi.common.table.ttl.model.TtlPolicyLevel;
import org.apache.hudi.common.table.ttl.model.TtlTriggerStrategy;
import org.apache.hudi.common.util.JsonUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.shell.Shell;

import java.io.IOException;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class of {@link TtlCommand}.
 */
@SpringBootTest(properties = {"spring.shell.interactive.enabled=false", "spring.shell.command.script.enabled=false"})
public class TestTtlCommand extends CLIFunctionalTestHarness {

  @Autowired
  private Shell shell;

  @BeforeEach
  public void init() throws IOException {
    String tableName = tableName();

    HoodieCLI.conf = hadoopConf();
    // Create table and connect
    new TableCommand().createTable(
        tablePath(tableName), tableName, HoodieTableType.COPY_ON_WRITE.name(),
        "", TimelineLayoutVersion.VERSION_1, "org.apache.hudi.common.model.HoodieAvroPayload");
  }

  @Test
  public void testTtlOnOff() {
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    HoodieTableConfig config = client.getTableConfig();
    assertFalse(config.isTtlPoliciesEnabled());

    Object cr = shell.evaluate(() -> "ttl_policy on");
    assertTrue(ShellEvaluationResultUtil.isSuccess(cr));
    assertEquals("TTL management turned ON", cr.toString());

    config = new HoodieTableConfig(client.getFs(), client.getMetaPath(), null, null);
    assertTrue(config.isTtlPoliciesEnabled());
    assertEquals(TtlTriggerStrategy.NUM_COMMITS.toString(), config.getTtlTriggerStrategy());
    assertEquals("10", config.getTtlTriggerValue());

    cr = shell.evaluate(() -> "ttl_policy off");
    assertTrue(ShellEvaluationResultUtil.isSuccess(cr));
    assertEquals("TTL management turned OFF", cr.toString());

    config = new HoodieTableConfig(client.getFs(), client.getMetaPath(), null, null);
    assertFalse(config.isTtlPoliciesEnabled());
  }

  @Test
  public void testTtlSettingsReconfigSuccess() {
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    HoodieTableConfig config = client.getTableConfig();
    // default config
    assertFalse(config.isTtlPoliciesEnabled());
    assertEquals(TtlTriggerStrategy.NUM_COMMITS.toString(), config.getTtlTriggerStrategy());
    assertEquals("10", config.getTtlTriggerValue());
    assertEquals(TtlPoliciesConflictResolutionRule.MAX_TTL.toString(), config.getTtlPoliciesConflictResolutionRule());

    // change settings success
    Object cr = shell.evaluate(() -> "ttl_policy settings --strategy TIME_ELAPSED --value 8 --resolveConflictsBy MIN_TTL");
    assertTrue(ShellEvaluationResultUtil.isSuccess(cr));
    assertEquals("TTL management configured with strategy = TIME_ELAPSED, value = 8 and conflictResolutionRule = MIN_TTL", cr.toString());

    // check changes
    config = new HoodieTableConfig(client.getFs(), client.getMetaPath(), null, null);
    assertFalse(config.isTtlPoliciesEnabled());
    assertEquals(TtlTriggerStrategy.TIME_ELAPSED.toString(), config.getTtlTriggerStrategy());
    assertEquals("8", config.getTtlTriggerValue());
    assertEquals(TtlPoliciesConflictResolutionRule.MIN_TTL.toString(), config.getTtlPoliciesConflictResolutionRule());

    // turn on
    cr = shell.evaluate(() -> "ttl_policy on");
    assertTrue(ShellEvaluationResultUtil.isSuccess(cr));

    // trigger does not change
    config = new HoodieTableConfig(client.getFs(), client.getMetaPath(), null, null);
    assertTrue(config.isTtlPoliciesEnabled());
    assertEquals(TtlTriggerStrategy.TIME_ELAPSED.toString(), config.getTtlTriggerStrategy());
    assertEquals("8", config.getTtlTriggerValue());
    assertEquals(TtlPoliciesConflictResolutionRule.MIN_TTL.toString(), config.getTtlPoliciesConflictResolutionRule());
  }

  @Test
  public void testTtlSettingsReconfigFailure() {
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();

    // no strategy
    Object cr = shell.evaluate(() -> "ttl_policy settings --value 8 --resolveConflictsBy MAX_TTL");
    assertFalse(ShellEvaluationResultUtil.isSuccess(cr));

    // invalid strategy
    cr = shell.evaluate(() -> "ttl_policy settings --strategy OLOLO --value 8 --resolveConflictsBy MAX_TTL");
    assertFalse(ShellEvaluationResultUtil.isSuccess(cr));
    String result = ((Throwable)cr).getMessage();
    assertTrue(result.contains("Strategy OLOLO is invalid. Valid TTL trigger strategies are: NUM_COMMITS, TIME_ELAPSED"));

    // no value
    cr = shell.evaluate(() -> "ttl_policy settings --strategy TIME_ELAPSED --resolveConflictsBy MAX_TTL");
    assertFalse(ShellEvaluationResultUtil.isSuccess(cr));

    //value is not a number
    cr = shell.evaluate(() -> "ttl_policy settings --strategy TIME_ELAPSED --value ololo --resolveConflictsBy MAX_TTL");
    assertFalse(ShellEvaluationResultUtil.isSuccess(cr));
    result = ((Throwable)cr).getMessage();
    assertTrue(result.contains("Trigger value must be integer greater than 0, but you tried to set ololo"));

    // value is not an integer
    cr = shell.evaluate(() -> "ttl_policy settings --strategy TIME_ELAPSED --value 3.5 --resolveConflictsBy MAX_TTL");
    assertFalse(ShellEvaluationResultUtil.isSuccess(cr));
    result = ((Throwable)cr).getMessage();
    assertTrue(result.contains("Trigger value must be integer greater than 0, but you tried to set 3.5"));

    // value is 0
    cr = shell.evaluate(() -> "ttl_policy settings --strategy TIME_ELAPSED --value 0 --resolveConflictsBy MAX_TTL");
    assertFalse(ShellEvaluationResultUtil.isSuccess(cr));
    result = ((Throwable)cr).getMessage();
    assertTrue(result.contains("Trigger value must be integer greater than 0, but you tried to set 0"));

    // no conflict resolution rule
    cr = shell.evaluate(() -> "ttl_policy settings --strategy TIME_ELAPSED --value 8 ");
    assertFalse(ShellEvaluationResultUtil.isSuccess(cr));

    // invalid conflict resolution rule
    cr = shell.evaluate(() -> "ttl_policy settings --strategy TIME_ELAPSED --value 8 --resolveConflictsBy OLOLO");
    assertFalse(ShellEvaluationResultUtil.isSuccess(cr));
    result = ((Throwable)cr).getMessage();
    assertTrue(result.contains("TTL policies conflict resolution rule OLOLO is invalid. Valid TTL policies conflict resolution rules are: MIN_TTL, MAX_TTL"));

    // no changes
    HoodieTableConfig  config = new HoodieTableConfig(client.getFs(), client.getMetaPath(), null, null);
    assertFalse(config.isTtlPoliciesEnabled());
    assertEquals(TtlTriggerStrategy.NUM_COMMITS.toString(), config.getTtlTriggerStrategy());
    assertEquals("10", config.getTtlTriggerValue());
    assertEquals(TtlPoliciesConflictResolutionRule.MAX_TTL.toString(), config.getTtlPoliciesConflictResolutionRule());
  }

  @Test
  public void testShowTtlPolicies() throws Exception {
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    Object cr = shell.evaluate(() -> "ttl_policy show");
    assertTrue(ShellEvaluationResultUtil.isSuccess(cr));
    String result = cr.toString();
    assertTrue(result.contains("(empty)"));

    TtlPolicyDAO ttlPolicyDAO = client.getTtlPolicyDAO();

    TtlPolicy policy1 = new TtlPolicy("*", TtlPolicyLevel.PARTITION, 12, ChronoUnit.MONTHS);
    String json = JsonUtils.toString(policy1);
    ttlPolicyDAO.save(json);
    cr = shell.evaluate(() -> "ttl_policy show");
    assertTrue(ShellEvaluationResultUtil.isSuccess(cr));
    result = cr.toString();
    assertEquals(5, result.split("\n").length); // 3(horizontal borders) + 1(header line) + 1(record line)

    TtlPolicy policy2 = new TtlPolicy("2023", TtlPolicyLevel.PARTITION, 2, ChronoUnit.YEARS);
    json = JsonUtils.toString(policy2);
    ttlPolicyDAO.save(json);
    cr = shell.evaluate(() -> "ttl_policy show");
    assertTrue(ShellEvaluationResultUtil.isSuccess(cr));
    result = cr.toString();
    assertEquals(7, result.split("\n").length); // 4(horizontal borders) + 1(header line) + 2(record line)
  }

  @Test
  public void testSaveTtlPolicy() throws Exception {
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    // mandatory arg (json) is not specified
    Object cr = shell.evaluate(() -> "ttl_policy save");
    assertFalse(ShellEvaluationResultUtil.isSuccess(cr));

    // json is empty string
    cr = shell.evaluate(() -> "ttl_policy save --json ''");
    assertFalse(ShellEvaluationResultUtil.isSuccess(cr));

    // json is invalid
    cr = shell.evaluate(() -> "ttl_policy save --json {\"spec\":\"*\"");
    assertFalse(ShellEvaluationResultUtil.isSuccess(cr));
    String result = ((Throwable)cr).getMessage();
    assertTrue(result.contains("Unable to parse: {\"spec\":\"*\""));

    // invalid fields
    TtlPolicy policy0 = new TtlPolicy(null, TtlPolicyLevel.RECORD, -30, null);
    String json1 = JsonUtils.toString(policy0);
    cr = shell.evaluate(() -> "ttl_policy save --json " + json1);
    assertFalse(ShellEvaluationResultUtil.isSuccess(cr));
    result = ((Throwable)cr).getMessage();
    assertTrue(result.contains("Invalid TTL policy: 'spec' must not be empty; 'value' must be integer greater than 0;"
        + " 'units' must not be null;"));

    policy0 = new TtlPolicy("1990/*", null, 0, null);
    String json2 = JsonUtils.toString(policy0);
    cr = shell.evaluate(() -> "ttl_policy save --json " + json2);
    assertFalse(ShellEvaluationResultUtil.isSuccess(cr));
    result = ((Throwable)cr).getMessage();
    assertTrue(result.contains("Invalid TTL policy: 'level' must not be null; 'value' must be integer greater than 0;"
        + " 'units' must not be null;"));

    // insert 1st
    TtlPolicy policy1 = new TtlPolicy("*", TtlPolicyLevel.PARTITION, 12, ChronoUnit.MONTHS);
    String json3 = JsonUtils.toString(policy1);
    cr = shell.evaluate(() -> "ttl_policy save --json " + json3);
    assertTrue(ShellEvaluationResultUtil.isSuccess(cr));
    result = cr.toString();
    assertTrue(result.contains(policy1.getLevel().toString()));
    assertEquals(5, result.split("\n").length); // 3(horizontal borders) + 1(header line) + 1(record line)
    TtlPolicyDAO ttlPolicyDAO = client.getTtlPolicyDAO();
    assertEquals(1, ttlPolicyDAO.getAll().size());

    // update 1st
    policy1.setLevel(TtlPolicyLevel.RECORD);
    String json4 = JsonUtils.toString(policy1);
    cr = shell.evaluate(() -> "ttl_policy save --json " + json4);
    assertTrue(ShellEvaluationResultUtil.isSuccess(cr));
    result = cr.toString();
    assertTrue(result.contains(policy1.getLevel().toString()));
    assertEquals(5, result.split("\n").length); // 3(horizontal borders) + 1(header line) + 1(record line)
    assertEquals(1, ttlPolicyDAO.getAll().size());

    // insert 2nd
    TtlPolicy policy2 = new TtlPolicy("2023", TtlPolicyLevel.PARTITION, 2, ChronoUnit.YEARS);
    String json5 = JsonUtils.toString(policy2);
    cr = shell.evaluate(() -> "ttl_policy save --json " + json5);
    assertTrue(ShellEvaluationResultUtil.isSuccess(cr));
    result = cr.toString();
    assertTrue(result.contains(policy1.getLevel().toString()));
    assertEquals(7, result.split("\n").length); // 4(horizontal borders) + 1(header line) + 2(record line)
    assertEquals(2, ttlPolicyDAO.getAll().size());

    // update 2nd
    policy2.setUnits(ChronoUnit.DAYS);
    String json6 = JsonUtils.toString(policy2);
    cr = shell.evaluate(() -> "ttl_policy save --json " + json6);
    assertTrue(ShellEvaluationResultUtil.isSuccess(cr));
    result = cr.toString();
    assertTrue(result.contains(policy2.getUnits().toString()));
    assertEquals(7, result.split("\n").length); // 4(horizontal borders) + 1(header line) + 2(record line)
    assertEquals(2, ttlPolicyDAO.getAll().size());
  }

  @Test
  public void testDeleteTtlPolicyBySpec() throws Exception {
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    // mandatory arg (spec) is not specified
    Object cr = shell.evaluate(() -> "ttl_policy delete");
    assertFalse(ShellEvaluationResultUtil.isSuccess(cr));

    // no policies
    cr = shell.evaluate(() -> "ttl_policy delete --spec *");
    assertFalse(ShellEvaluationResultUtil.isSuccess(cr));
    String result = ((Throwable)cr).getMessage();
    assertTrue(result.contains("The list of TTL policies is empty."));

    // add policy
    TtlPolicyDAO ttlPolicyDAO = client.getTtlPolicyDAO();
    TtlPolicy policy1 = new TtlPolicy("year=2023/month=12", TtlPolicyLevel.PARTITION, 12, ChronoUnit.MONTHS);
    String json = JsonUtils.toString(policy1);
    ttlPolicyDAO.save(json);

    // policy with such spec does not exist
    cr = shell.evaluate(() -> "ttl_policy delete --spec 2023");
    assertFalse(ShellEvaluationResultUtil.isSuccess(cr));
    result = ((Throwable)cr).getMessage();
    assertTrue(result.contains("There is no TTL policy with spec: 2023"));

    // successful delete
    cr = shell.evaluate(() -> "ttl_policy delete --spec year=2023/month=12");
    assertTrue(ShellEvaluationResultUtil.isSuccess(cr));
    assertTrue(cr.toString().contains("(empty)"));
  }

  @Test
  public void testEmptyTtlPolicies() throws Exception {
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    Object cr = shell.evaluate(() -> "ttl_policy empty");
    assertTrue(ShellEvaluationResultUtil.isSuccess(cr));
    String result = cr.toString();
    assertTrue(result.contains("(empty)"));

    TtlPolicyDAO ttlPolicyDAO = client.getTtlPolicyDAO();
    TtlPolicy policy1 = new TtlPolicy("*", TtlPolicyLevel.PARTITION, 12, ChronoUnit.MONTHS);
    String json = JsonUtils.toString(policy1);
    ttlPolicyDAO.save(json);
    shell.evaluate(() -> "ttl_policy show");

    cr = shell.evaluate(() -> "ttl_policy empty");
    assertTrue(ShellEvaluationResultUtil.isSuccess(cr));
    result = cr.toString();
    assertTrue(result.contains("(empty)"));
  }
}
