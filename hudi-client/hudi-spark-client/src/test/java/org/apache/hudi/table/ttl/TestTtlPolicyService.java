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

import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.ttl.TtlConfigurer;
import org.apache.hudi.common.table.ttl.TtlPolicyDAO;
import org.apache.hudi.common.table.ttl.model.TtlPolicy;
import org.apache.hudi.common.table.ttl.model.TtlPolicyLevel;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.InvalidTtlPolicyException;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;
import org.apache.hudi.testutils.providers.SparkProvider;

import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests {@link TtlPolicyService}.
 */
public class TestTtlPolicyService extends HoodieSparkClientTestHarness implements SparkProvider {

  private TtlPolicyService ttlPolicyService;
  private TtlPolicyDAO ttlPolicyDAO;

  @BeforeEach
  public void setUp() throws Exception {
    boolean initialized = sparkSession != null;
    if (!initialized) {
      SparkConf sparkConf = conf();
      sparkConf.registerKryoClasses(new Class[]{HoodieWriteConfig.class, HoodieRecord.class, HoodieKey.class});
      HoodieReadClient.addHoodieSupport(sparkConf);
      sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
      sqlContext = sparkSession.sqlContext();
      jsc = new JavaSparkContext(sparkSession.sparkContext());
      context = new HoodieSparkEngineContext(jsc);
    }
    initPath();
    initMetaClient();
    this.ttlPolicyDAO = metaClient.getTtlPolicyDAO();
    this.ttlPolicyService = new TtlPolicyService(metaClient, context,
        HoodieSparkTable.create(HoodieWriteConfig.newBuilder().withPath(basePath).build(), context, metaClient));
  }

  @AfterEach
  public void cleanupSparkContexts() {
    super.cleanupSparkContexts();
  }

  protected void initMetaClient() throws IOException {
    String rootPathStr = "file://" + tempDir.toAbsolutePath();
    Path rootPath = new Path(rootPathStr);
    rootPath.getFileSystem(jsc.hadoopConfiguration()).mkdirs(rootPath);
    metaClient = HoodieTestUtils.init(rootPathStr, getTableType());
    basePath = metaClient.getBasePathV2().toString();
  }

  @Test
  public void testRunWithNoPolicies() {
    assertThrows(InvalidTtlPolicyException.class, () -> ttlPolicyService.run(true), "TTL policies are not configured");
    assertThrows(InvalidTtlPolicyException.class, () -> ttlPolicyService.run(false), "TTL policies are not configured");
    Assertions.assertDoesNotThrow(() -> ttlPolicyService.runInline()); // because of TTL is OFF
    TtlConfigurer.setAndGetTtlConf(metaClient.getFs(), metaClient.getMetaPath(), "true", null, null, null);
    Assertions.assertDoesNotThrow(() -> ttlPolicyService.runInline()); // because of triggering conditions
  }

  @Test
  public void testRunWithDisabledTtl() throws IOException {
    ttlPolicyDAO.save(JsonUtils.toString(new TtlPolicy("*", TtlPolicyLevel.PARTITION, 12, ChronoUnit.MONTHS)));
    Assertions.assertDoesNotThrow(() -> ttlPolicyService.run(true));
    Assertions.assertDoesNotThrow(() -> ttlPolicyService.run(false));
    Assertions.assertDoesNotThrow(() -> ttlPolicyService.runInline());
  }

  @Test
  public void testShouldNotTriggerOnEmptyTimelineAndDefaultConfig() throws IOException {
    ttlPolicyDAO.save(JsonUtils.toString(new TtlPolicy("*", TtlPolicyLevel.PARTITION, 12, ChronoUnit.MONTHS)));
    TtlConfigurer.setAndGetTtlConf(metaClient.getFs(), metaClient.getMetaPath(), "true", null, null, null);
    ttlPolicyService = new TtlPolicyService(metaClient, context,
        HoodieSparkTable.create(HoodieWriteConfig.newBuilder().withPath(basePath).build(), context, metaClient));
    Assertions.assertDoesNotThrow(() -> ttlPolicyService.runInline());
  }

  @Override
  public HoodieEngineContext context() {
    return context;
  }

  @Override
  public SparkSession spark() {
    return sparkSession;
  }

  @Override
  public SQLContext sqlContext() {
    return sqlContext;
  }

  @Override
  public JavaSparkContext jsc() {
    return jsc;
  }
}
