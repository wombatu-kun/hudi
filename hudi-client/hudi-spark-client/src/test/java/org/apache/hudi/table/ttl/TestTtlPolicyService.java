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
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.ttl.TtlConfigurer;
import org.apache.hudi.common.table.ttl.TtlPolicyComparator;
import org.apache.hudi.common.table.ttl.TtlPolicyDAO;
import org.apache.hudi.common.table.ttl.model.TtlPoliciesConflictResolutionRule;
import org.apache.hudi.common.table.ttl.model.TtlPolicy;
import org.apache.hudi.common.table.ttl.model.TtlPolicyLevel;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.InvalidTtlPolicyException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;
import org.apache.hudi.testutils.providers.SparkProvider;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests {@link TtlPolicyService}.
 */
public class TestTtlPolicyService extends HoodieSparkClientTestHarness implements SparkProvider {

  private TtlPolicyService ttlPolicyService;
  private TtlPolicyDAO ttlPolicyDAO;
  private HoodieTableMetaClient metaClient;
  private SparkRDDWriteClient<HoodieAvroPayload> sparkClient;
  private static final String SCHEMA = "{\"type\":\"record\",\"name\":\"partitionTtlTable\",\"fields\":["
      + "{\"name\":\"timestamp\",\"type\":\"string\"},{\"name\":\"row_key\",\"type\":\"string\"},{\"name\":\"partition_path\",\"type\":\"string\"}]}";
  private final Schema avroSchema = new Schema.Parser().parse(SCHEMA);

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
    if (!initialized) {
      // should be after initMetaClient(), otherwise wrong basePath would be used
      HoodieWriteConfig.Builder builder = HoodieWriteConfig.newBuilder()
          .withPath(basePath)
          .withSchema(SCHEMA)
          .withParallelism(2, 2).withBulkInsertParallelism(2).withFinalizeWriteParallelism(2).withDeleteParallelism(2)
          .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024).build())
          .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024 * 1024).parquetMaxFileSize(1024 * 1024).orcMaxFileSize(1024 * 1024).build())
          .forTable("partition-ttl-table");

      // initialize Spark client to upsert data
      TypedProperties properties = new TypedProperties();
      properties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "row_key");
      properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition_path");
      properties.put(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key(), "false");
      properties.put(HoodieWriteConfig.AUTO_COMMIT_ENABLE.key(), "true");
      properties.put(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "1");
      properties.put(HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key(), "3");
      properties.put(HoodieArchivalConfig.MIN_COMMITS_TO_KEEP.key(), "2");
      properties.put(HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key(), "1");
      properties.put(HoodieCleanConfig.ALLOW_MULTIPLE_CLEANS.key(), "false");
      HoodieWriteConfig config = builder.withProps(properties).build();
      this.sparkClient = new SparkRDDWriteClient<>(context, config);
    }
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
    this.metaClient = HoodieTestUtils.init(rootPathStr, HoodieTableType.MERGE_ON_READ);
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

  private HoodieAvroRecord<HoodieAvroPayload> prepareRecordForCommit(String recordKey, String partitionPath, String timestamp) {
    GenericRecord record = new GenericData.Record(avroSchema);
    record.put("row_key", recordKey);
    record.put("partition_path", partitionPath);
    record.put("timestamp", timestamp);
    HoodieAvroPayload avroPayload = new HoodieAvroPayload(Option.of(record));
    HoodieKey hoodieKey = new HoodieKey(recordKey, partitionPath);
    return new HoodieAvroRecord<>(hoodieKey, avroPayload, HoodieOperation.INSERT);
  }

  private ArrayList<String> prepareTimestampsForCommits(int daysFromCurrentMinus) {
    ArrayList<String> timestampsFromNowMinus = new ArrayList<>();
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(new Date());
    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmssSSS");
    IntStream.range(0, daysFromCurrentMinus + 1).forEach(d -> {
      timestampsFromNowMinus.add(formatter.format(new Timestamp((calendar.getTime()).getTime())));
      calendar.add(Calendar.DATE, -1);
    });
    return timestampsFromNowMinus;
  }

  @Test
  public void testConflictResolutionStrategy() throws ParseException {
    // Timeline:
    // 2 days ago - deltacommit 1 - + 2 new partitions (1 and 2 days), both are outdated - 2 expired
    // yesterday  - deltacommit 2 - + 2 new partitions (similar), only one is outdated   - 3 expired
    // current    - deltacommit 3 - + 2 new partitions (similar), both are not outdated  - 3 expired
    ArrayList<String> timestampsFromNowMinus = prepareTimestampsForCommits(2);

    List<TtlPolicy> policies = new ArrayList<>();
    policies.add(new TtlPolicy("*", TtlPolicyLevel.PARTITION, 1, ChronoUnit.DAYS));
    policies.add(new TtlPolicy("ttl2d/*", TtlPolicyLevel.PARTITION, 2, ChronoUnit.DAYS));
    // list of policies should be sorted by TtlPoliciesConflictResolutionRule
    // need to sort the list manually because we're not calling them from TtlPolicyDAO
    policies.sort(new TtlPolicyComparator(TtlPoliciesConflictResolutionRule.MAX_TTL));

    // deltacommit 1, 2 days ago
    String commitTime = timestampsFromNowMinus.get(2);
    HoodieAvroRecord<HoodieAvroPayload> rec1 = prepareRecordForCommit("01_ttl1d_minus_2days", "ttl1d/minus_2days", commitTime);
    HoodieAvroRecord<HoodieAvroPayload> rec2 = prepareRecordForCommit("02_ttl2d_minus_2days", "ttl2d/minus_2days", commitTime);
    sparkClient.startCommitWithTime(commitTime);
    JavaRDD<HoodieRecord<HoodieAvroPayload>> recordList = jsc.parallelize(Arrays.asList(rec1, rec2), 1);
    sparkClient.upsert(recordList, commitTime);
    metaClient.reloadActiveTimeline();
    assertEquals(2, ttlPolicyService.findExpiredPartitions(policies, 10).size());

    // deltacommit 2, yesterday
    commitTime = timestampsFromNowMinus.get(1);
    rec1 = prepareRecordForCommit("02_ttl1d_yesterday", "ttl1d/yesterday", commitTime);
    rec2 = prepareRecordForCommit("03_ttl2d_yesterday", "ttl2d/yesterday", commitTime);
    sparkClient.startCommitWithTime(commitTime);
    recordList = jsc.parallelize(Arrays.asList(rec1, rec2), 1);
    sparkClient.upsert(recordList, commitTime);
    metaClient.reloadActiveTimeline();
    assertEquals(3, ttlPolicyService.findExpiredPartitions(policies, 10).size());

    // deltacommit 3, current time
    commitTime = timestampsFromNowMinus.get(0);
    rec1 = prepareRecordForCommit("04_ttl1d_current", "ttl1d/current", commitTime);
    rec2 = prepareRecordForCommit("05_ttl2d_current", "ttl2d/current", commitTime);
    sparkClient.startCommitWithTime(commitTime);
    recordList = jsc.parallelize(Arrays.asList(rec1, rec2), 1);
    sparkClient.upsert(recordList, commitTime);
    metaClient.reloadActiveTimeline();
    assertEquals(3, ttlPolicyService.findExpiredPartitions(policies, 10).size());
  }

  @Test
  public void testFilterSavePoint() throws ParseException {
    // Timeline:
    // 2 days ago - deltacommit 1 - 1 expired
    // yesterday  - deltacommit 2 - 2 expired
    //              save point    - 0 expired, all partitions before a save point would be filtered
    ArrayList<String> timestampsFromNowMinus = prepareTimestampsForCommits(2);

    List<TtlPolicy> policies = new ArrayList<>();
    policies.add(new TtlPolicy("*", TtlPolicyLevel.PARTITION, 1, ChronoUnit.DAYS));

    // deltacommit 1, 2 days ago
    String commitTime = timestampsFromNowMinus.get(2);
    HoodieAvroRecord<HoodieAvroPayload> rec1 = prepareRecordForCommit("01_before_savepoint", "ttl1d/before_savepoint", commitTime);
    sparkClient.startCommitWithTime(commitTime);
    JavaRDD<HoodieRecord<HoodieAvroPayload>> recordList = jsc.parallelize(Collections.singletonList(rec1), 1);
    sparkClient.upsert(recordList, commitTime);

    // deltacommit 2, yesterday
    commitTime = timestampsFromNowMinus.get(1);
    rec1 = prepareRecordForCommit("02_savepoint", "ttl1d/savepoint", commitTime);
    sparkClient.startCommitWithTime(commitTime);
    recordList = jsc.parallelize(Collections.singletonList(rec1), 1);
    sparkClient.upsert(recordList, commitTime);
    metaClient.reloadActiveTimeline();
    assertEquals(2, ttlPolicyService.findExpiredPartitions(policies, 10).size());

    // create save point, yesterday
    sparkClient.savepoint(commitTime, "test_user", "test_save_point");
    metaClient.reloadActiveTimeline();
    assertEquals(0, ttlPolicyService.findExpiredPartitions(policies, 10).size());
  }

  @Test
  public void testFilterClustering() throws ParseException {
    // Timeline:
    // 2 days ago - deltacommit 1 - 1 expired
    // yesterday  - deltacommit 2 - 2 expired
    //        schedule clustering - 0 expired, all partitions before would be filtered
    ArrayList<String> timestampsFromNowMinus = prepareTimestampsForCommits(2);

    List<TtlPolicy> policies = new ArrayList<>();
    policies.add(new TtlPolicy("*", TtlPolicyLevel.PARTITION, 1, ChronoUnit.DAYS));

    // deltacommit 1, 2 days ago
    String commitTime = timestampsFromNowMinus.get(2);
    HoodieAvroRecord<HoodieAvroPayload> rec1 = prepareRecordForCommit("01_before_clustering", "ttl1d/before_clustering", commitTime);
    sparkClient.startCommitWithTime(commitTime);
    JavaRDD<HoodieRecord<HoodieAvroPayload>> recordList = jsc.parallelize(Collections.singletonList(rec1), 1);
    sparkClient.upsert(recordList, commitTime);

    // deltacommit 2, yesterday
    commitTime = timestampsFromNowMinus.get(1);
    rec1 = prepareRecordForCommit("02_clustering", "ttl1d/clustering", commitTime);
    sparkClient.startCommitWithTime(commitTime);
    recordList = jsc.parallelize(Collections.singletonList(rec1), 1);
    sparkClient.upsert(recordList, commitTime);
    metaClient.reloadActiveTimeline();
    assertEquals(2, ttlPolicyService.findExpiredPartitions(policies, 10).size());

    // schedule clustering
    sparkClient.scheduleClustering(Option.empty());
    metaClient.reloadActiveTimeline();
    assertEquals(0, ttlPolicyService.findExpiredPartitions(policies, 10).size());
  }

  @Test
  public void testFilterCompaction() throws ParseException {
    assertEquals(HoodieTableType.MERGE_ON_READ, metaClient.getTableType());

    // Timeline:
    // 4 days ago - deltacommit 1, base file 1
    // 3 days ago - deltacommit 2, base file 2
    // 2 days ago - deltacommit 3, log file 1 to base file 2
    // yesterday  - deltacommit 4, log file 2 to base file 2
    //        schedule compaction - 1 expired, only affected partitions (with base file 2) would be filtered
    ArrayList<String> timestampsFromNowMinus = prepareTimestampsForCommits(4);

    List<TtlPolicy> policies = new ArrayList<>();
    policies.add(new TtlPolicy("*", TtlPolicyLevel.PARTITION, 1, ChronoUnit.DAYS));

    // deltacommit 1, 4 days ago
    String commitTime = timestampsFromNowMinus.get(4);
    HoodieAvroRecord<HoodieAvroPayload> rec1 = prepareRecordForCommit("01_not_used_in_compaction", "ttl1d/not_used_in_compaction", commitTime);
    sparkClient.startCommitWithTime(commitTime);
    JavaRDD<HoodieRecord<HoodieAvroPayload>> recordList = jsc.parallelize(Collections.singletonList(rec1), 1);
    sparkClient.upsert(recordList, commitTime);

    // deltacommit 2, base file, 3 days ago
    commitTime = timestampsFromNowMinus.get(3);
    rec1 = prepareRecordForCommit("02_compaction", "ttl1d/compaction", commitTime);
    sparkClient.startCommitWithTime(commitTime);
    recordList = jsc.parallelize(Collections.singletonList(rec1), 1);
    sparkClient.upsert(recordList, commitTime);

    // deltacommit 3, log file 1, 2 days ago
    commitTime = timestampsFromNowMinus.get(2);
    rec1 = prepareRecordForCommit("02_compaction", "ttl1d/compaction", commitTime);
    sparkClient.startCommitWithTime(commitTime);
    recordList = jsc.parallelize(Collections.singletonList(rec1), 1);
    sparkClient.upsert(recordList, commitTime);

    // deltacommit 4, log file 2, yesterday
    commitTime = timestampsFromNowMinus.get(1);
    rec1 = prepareRecordForCommit("02_compaction", "ttl1d/compaction", commitTime);
    sparkClient.startCommitWithTime(commitTime);
    recordList = jsc.parallelize(Collections.singletonList(rec1), 1);
    sparkClient.upsert(recordList, commitTime);
    metaClient.reloadActiveTimeline();
    assertEquals(2, ttlPolicyService.findExpiredPartitions(policies, 10).size());

    // schedule compaction
    sparkClient.scheduleCompaction(Option.empty());
    metaClient.reloadActiveTimeline();
    assertEquals(1, ttlPolicyService.findExpiredPartitions(policies, 10).size());
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
