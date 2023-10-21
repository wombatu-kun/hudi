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
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.cli.utils.InputStreamConsumer;
import org.apache.hudi.cli.utils.SparkUtil;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.ttl.TtlConfigurer;

import org.apache.spark.launcher.SparkLauncher;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * CLI command to configure TTL policies.
 */
@ShellComponent
public class TtlCommand {

  @ShellMethod(key = "ttl_policy on", value = "Turns on TTL management")
  public String enableTtlManagement() {
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    TtlConfigurer.setAndGetTtlConf(client.getFs(), client.getMetaPath(), "true", null, null, null);
    return "TTL management turned ON";
  }

  @ShellMethod(key = "ttl_policy off", value = "Turns off TTL management")
  public String disableTtlManagement() {
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    TtlConfigurer.setAndGetTtlConf(client.getFs(), client.getMetaPath(),"false", null, null, null);
    return "TTL management turned OFF";
  }

  @ShellMethod(key = "ttl_policy settings", value = "Updates TTL configuration settings (does not change on/off property)")
  public String updateSettings(
      @ShellOption(value = {"--strategy"}, help = "Trigger strategy: NUM_COMMITS or TIME_ELAPSED") final String strategy,
      @ShellOption(value = {"--value"}, help = "Value for trigger strategy: number of commits or elapsed DAYS") final String value,
      @ShellOption(value = {"--resolveConflictsBy"}, help = "Value for policies conflict resolution: MAX_TTL or MIN_TTL") final String resolveConflictsBy) {
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    TtlConfigurer.setAndGetTtlConf(client.getFs(), client.getMetaPath(),null, strategy, value, resolveConflictsBy);
    return "TTL management configured with strategy = " + strategy + ", value = " + value + " and conflictResolutionRule = " + resolveConflictsBy;
  }

  @ShellMethod(key = "ttl_policy show", value = "Fetches all configured TTL policies")
  public String showPolicies() throws IOException {
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    TableHeader header = new TableHeader().addTableHeaderField("spec").addTableHeaderField("level")
        .addTableHeaderField("value").addTableHeaderField("units").addTableHeaderField("useSince");
    List<Comparable[]> rows = new ArrayList<>();

    client.getTtlPolicyDAO().getAll().forEach(p ->
        rows.add(new Comparable[] { p.getSpec(), p.getLevel(), p.getValue(), p.getUnits(), p.getUseSince() })
    );
    return HoodiePrintHelper.print(header, new HashMap<>(), "", false, -1, false, rows);
  }

  @ShellMethod(key = "ttl_policy save", value = "Upserts TTL policy")
  public String savePolicy(
      @ShellOption(value = {"--json"}, help = "JSON representation of TTL policy") final String json) throws IOException {
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    client.getTtlPolicyDAO().save(json);
    return showPolicies();
  }

  @ShellMethod(key = "ttl_policy delete", value = "Deletes TTL policy by spec")
  public String deleteBySpec(
      @ShellOption(value = {"--spec"}, help = "Value of spec field of the TTL policy") final String spec) throws IOException {
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    client.getTtlPolicyDAO().deleteBySpec(spec);
    return showPolicies();
  }

  @ShellMethod(key = "ttl_policy empty", value = "Deletes all configured TTL policies")
  public String emptyPolicies() throws IOException {
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    client.getTtlPolicyDAO().deleteAll();
    return showPolicies();
  }

  @ShellMethod(key = "ttl_policy run", value = "Runs existing TTL policies processing")
  public String runTtlProcessing(
      @ShellOption(value = {"--dryRun"}, help = "Dry-run: true or false") final String dryRun,
      @ShellOption(value = {"--sparkProperties"}, help = "Spark Properties File Path") final String sparkPropertiesPath,
      @ShellOption(value = {"--sparkMaster"}, defaultValue = "", help = "Spark Master") final String master,
      @ShellOption(value = {"--sparkMemory"}, defaultValue = "1G", help = "Spark executor memory") final String sparkMemory
  ) throws Exception {
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    String tablePath = metaClient.getBasePathV2().toString();
    SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
    sparkLauncher.addAppArgs(SparkMain.SparkCommand.TTL_RUN.toString(), master, sparkMemory, dryRun, tablePath);
    Process process = sparkLauncher.launch();
    InputStreamConsumer.captureOutput(process);
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      return "Could not run TTL processing";
    } else {
      return "TTL processed successfully";
    }
  }
}
