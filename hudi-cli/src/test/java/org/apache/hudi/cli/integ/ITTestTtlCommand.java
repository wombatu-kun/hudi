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

package org.apache.hudi.cli.integ;

import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.commands.TableCommand;
import org.apache.hudi.cli.commands.TtlCommand;
import org.apache.hudi.cli.testutils.HoodieCLIIntegrationTestBase;
import org.apache.hudi.cli.testutils.ShellEvaluationResultUtil;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.ttl.TtlPolicyDAO;
import org.apache.hudi.common.table.ttl.model.TtlPolicy;
import org.apache.hudi.common.table.ttl.model.TtlPolicyLevel;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.shell.Shell;

import java.io.IOException;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class of {@link TtlCommand}.
 */
@SpringBootTest(properties = {"spring.shell.interactive.enabled=false", "spring.shell.command.script.enabled=false"})
public class ITTestTtlCommand extends HoodieCLIIntegrationTestBase {

  @Autowired
  private Shell shell;

  @BeforeEach
  public void init() throws IOException {
    String tableName = "test_table";

    // Create table and connect
    new TableCommand().createTable(
        basePath + Path.SEPARATOR + tableName, "test_table", HoodieTableType.COPY_ON_WRITE.name(),
        "", TimelineLayoutVersion.VERSION_1, "org.apache.hudi.common.model.HoodieAvroPayload");
  }

  @Test
  public void testRunTtlProcessing() throws IOException {
    Object cr = shell.evaluate(() -> "ttl_policy run --dryRun true --sparkMaster local");
    assertFalse(ShellEvaluationResultUtil.isSuccess(cr));

    TtlPolicyDAO ttlPolicyDAO = HoodieCLI.getTableMetaClient().getTtlPolicyDAO();
    TtlPolicy policy = new TtlPolicy("*", TtlPolicyLevel.PARTITION, 12, ChronoUnit.MONTHS);
    ttlPolicyDAO.save(policy);

    cr = shell.evaluate(() -> "ttl_policy run --dryRun true --sparkMaster local");
    assertTrue(ShellEvaluationResultUtil.isSuccess(cr));
  }
}
