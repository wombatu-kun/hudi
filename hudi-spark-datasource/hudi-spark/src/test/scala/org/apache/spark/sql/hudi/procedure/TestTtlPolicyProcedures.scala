/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.hudi.procedure

import org.apache.hadoop.fs.Path
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.model.{HoodiePartitionMetadata, HoodieReplaceCommitMetadata, WriteOperationType}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator
import org.apache.hudi.common.table.ttl.TtlConfigurer
import org.apache.hudi.common.table.ttl.model.{TtlPoliciesConflictResolutionRule, TtlPolicy, TtlPolicyLevel}
import org.apache.hudi.common.util.{JsonUtils, Option}
import org.apache.hudi.exception.InvalidTtlPolicyException
import org.apache.hudi.testutils.HoodieClientTestUtils
import org.apache.spark.sql.hudi.HoodieSparkSqlTestBase
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertTrue}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

class TestTtlPolicyProcedures extends HoodieSparkSqlTestBase {

  test("Test Call ttl_policy_show") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${new Path(tmp.getCanonicalPath, tableName).toUri.toString}"
      createTable(tableName, tablePath)

      // no policies
      var rows = spark.sql(s"call ttl_policy_show(table => '$tableName')").collect()
      assert(rows.length == 0)
      rows = spark.sql(s"call ttl_policy_show(path => '$tablePath')").collect()
      assert(rows.length == 0)

      val metaClient = HoodieTableMetaClient.builder().setBasePath(tablePath).setConf(spark.sessionState.newHadoopConf()).build()
      val ttlPolicyDAO = metaClient.getTtlPolicyDAO

      // 1 row
      val policy1 = new TtlPolicy("*", TtlPolicyLevel.PARTITION, 12, ChronoUnit.MONTHS)
      var json = JsonUtils.toString(policy1)
      ttlPolicyDAO.save(json)
      rows = spark.sql(s"call ttl_policy_show(table => '$tableName')").collect()
      rows.foreach(r => println(r))
      assert(rows.length == 1)
      assertEquals(policy1.getSpec, rows(0).get(0))
      assertEquals(policy1.getLevel.toString, rows(0).get(1))
      assertEquals(policy1.getValue.toString, rows(0).get(2))
      assertEquals(policy1.getUnits.toString, rows(0).get(3))
      assertNotNull(rows(0).get(0))

      // 2 rows
      val policy2: TtlPolicy = new TtlPolicy("2023", TtlPolicyLevel.PARTITION, 2, ChronoUnit.YEARS)
      json = JsonUtils.toString(policy2)
      ttlPolicyDAO.save(json)
      rows = spark.sql(s"call ttl_policy_show(table => '$tableName')").collect()
      rows.foreach(r => println(r))
      assert(rows.length == 2)
    }
  }

  test("Test Call ttl_policy_empty") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${new Path(tmp.getCanonicalPath, tableName).toUri.toString}"
      createTable(tableName, tablePath)

      // no policies
      var rows = spark.sql(s"call ttl_policy_empty(path => '$tablePath')").collect()
      assert(rows.length == 1)
      assert(rows(0).get(0) == "SUCCESS")
      // ttl_policy_empty with no policies
      spark.sql(s"call ttl_policy_empty(table => '$tableName')").collect()

      val metaClient = HoodieTableMetaClient.builder().setBasePath(tablePath).setConf(spark.sessionState.newHadoopConf()).build()
      val ttlPolicyDAO = metaClient.getTtlPolicyDAO

      // add 2 rows
      val policy1 = new TtlPolicy("*", TtlPolicyLevel.PARTITION, 12, ChronoUnit.MONTHS)
      var json = JsonUtils.toString(policy1)
      ttlPolicyDAO.save(json)
      val policy2: TtlPolicy = new TtlPolicy("2023", TtlPolicyLevel.PARTITION, 2, ChronoUnit.YEARS)
      json = JsonUtils.toString(policy2)
      ttlPolicyDAO.save(json)

      // with policies
      rows = spark.sql(s"call ttl_policy_empty(path => '$tablePath')").collect()
      assert(rows.length == 1)
      assert(rows(0).get(0) == "SUCCESS")
      assert(ttlPolicyDAO.getAll.isEmpty)
    }
  }

  test("Test Call ttl_policy_delete") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${new Path(tmp.getCanonicalPath, tableName).toUri.toString}"
      createTable(tableName, tablePath)

      // spec not specified
      assertThrows[AssertionError] {
        spark.sql(s"call ttl_policy_delete(path => '$tablePath')").collect()
      }
      // empty spec
      assertThrows[InvalidTtlPolicyException] {
        spark.sql(s"call ttl_policy_delete(table => '$tableName', spec => '')").collect()
      }
      // no policies
      assertThrows[InvalidTtlPolicyException] {
        spark.sql(s"call ttl_policy_delete(table => '$tableName', spec => '*')").collect()
      }

      val metaClient = HoodieTableMetaClient.builder().setBasePath(tablePath).setConf(spark.sessionState.newHadoopConf()).build()
      val ttlPolicyDAO = metaClient.getTtlPolicyDAO
      // add 2 rows
      val policy1 = new TtlPolicy("*", TtlPolicyLevel.PARTITION, 12, ChronoUnit.MONTHS)
      var json = JsonUtils.toString(policy1)
      ttlPolicyDAO.save(json)
      val policy2: TtlPolicy = new TtlPolicy("2023", TtlPolicyLevel.PARTITION, 2, ChronoUnit.YEARS)
      json = JsonUtils.toString(policy2)
      ttlPolicyDAO.save(json)

      // no such policy
      assertThrows[InvalidTtlPolicyException] {
        spark.sql(s"call ttl_policy_delete(table => '$tableName', spec => '!@#')").collect()
      }

      // delete by spec
      var rows = spark.sql(s"call ttl_policy_delete(path => '$tablePath', spec => '2023')").collect()
      assert(rows.length == 1)
      assert(rows(0).get(0) == policy2.getSpec)
      assert(rows(0).get(1) == "DELETED")
      rows = spark.sql(s"call ttl_policy_delete(path => '$tablePath', spec => '*')").collect()
      assert(rows.length == 1)
      assert(rows(0).get(0) == policy1.getSpec)
      assert(rows(0).get(1) == "DELETED")
      assert(ttlPolicyDAO.getAll.isEmpty)
    }
  }

  test("Test Call ttl_policy_save") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${new Path(tmp.getCanonicalPath, tableName).toUri.toString}"
      createTable(tableName, tablePath)

      // something not specified
      assertThrows[AssertionError] {
        spark.sql(s"call ttl_policy_save(path => '$tablePath')").collect()
      }
      assertThrows[AssertionError] {
        spark.sql(s"call ttl_policy_save(path => '$tablePath', spec => '*')").collect()
      }
      assertThrows[AssertionError] {
        spark.sql(s"call ttl_policy_save(path => '$tablePath', spec => '*', value => '1')").collect()
      }
      assertThrows[AssertionError] {
        spark.sql(s"call ttl_policy_save(path => '$tablePath', spec => '*', value => '1', level => 'PARTITION')").collect()
      }
      assertThrows[AssertionError] {
        spark.sql(s"call ttl_policy_save(path => '$tablePath', spec => '*', value => '1', units => 'HOURS')").collect()
      }
      assertThrows[AssertionError] {
        spark.sql(s"call ttl_policy_save(path => '$tablePath', spec => '*', units => 'DAYS', level => 'PARTITION')").collect()
      }
      assertThrows[AssertionError] {
        spark.sql(s"call ttl_policy_save(path => '$tablePath', units => 'YEARS', value => '1', level => 'PARTITION')").collect()
      }

      // something is empty
      assertThrows[InvalidTtlPolicyException] {
        spark.sql(s"call ttl_policy_save(table => '$tableName', spec => '', units => 'YEARS', value => '1', level => 'PARTITION')").collect()
      }
      assertThrows[IllegalArgumentException] {
        spark.sql(s"call ttl_policy_save(table => '$tableName', spec => '*', units => '', value => '1', level => 'PARTITION')").collect()
      }
      assertThrows[IllegalArgumentException] {
        spark.sql(s"call ttl_policy_save(table => '$tableName', spec => '*', units => 'MONTHS', value => '', level => 'PARTITION')").collect()
      }
      assertThrows[IllegalArgumentException] {
        spark.sql(s"call ttl_policy_save(table => '$tableName', spec => '*', units => 'DAYS', value => '1', level => '')").collect()
      }

      // PREPARE METADATA: insert some data and create partition 'dt=2021-01-05' with lastUpdateTime 29 days ago
      val ts: String = preparePartition(tableName, tablePath, "2021-01-05", 29L, 1)

      // add 1, TTL = 1day, so partition updated 29 days ago should be shown
      var rows = spark.sql(s"call ttl_policy_save(table => '$tableName', spec => '*', units => 'DAYS', value => '1', level => 'PARTITION')").collect()
      assert(rows.length == 3)
      assert(rows(0).get(0) == "POLICY")
      assert(rows(0).get(1) == "SAVED")
      assert(rows(0).get(2) == "*")
      assert(rows(0).get(3) == "PARTITION")
      assert(rows(0).get(4) == "1")
      assert(rows(0).get(5) == "DAYS")
      assert(rows(1).get(0) == "if run it")
      assert(rows(1).get(1) == "immediately")
      assert(rows(1).get(2) == "will")
      assert(rows(1).get(3) == "delete")
      assert(rows(1).get(4) == "partitions")
      assert(rows(1).get(5) == "(sample):")
      assert(rows(2).get(0) == "dt=2021-01-05")
      assert(rows(2).get(1) == ts)
      assert(rows(2).get(2) == "*")
      assert(rows(2).get(3) == "PARTITION")
      assert(rows(2).get(4) == "1")
      assert(rows(2).get(5) == "Days")
      // add 2, TTL = 1day but spec does not match, so partition 2021-01-05 should be shown
      rows = spark.sql(s"call ttl_policy_save(table => '$tableName', spec => '2023', units => 'DAYS', value => '1', level => 'PARTITION')").collect()
      assert(rows.length == 2)
      assert(rows(0).get(2) == "2023")
      assert(rows(0).get(3) == "PARTITION")
      assert(rows(0).get(4) == "1")
      assert(rows(0).get(5) == "DAYS")
      // update 1, TTL = 1month, so partition updated 29 days ago should not be shown
      rows = spark.sql(s"call ttl_policy_save(table => '$tableName', spec => '*', units => 'MONTHS', value => '1', level => 'PARTITION')").collect()
      assert(rows.length == 2)
      assert(rows(0).get(2) == "*")
      assert(rows(0).get(3) == "PARTITION")
      assert(rows(0).get(4) == "1")
      assert(rows(0).get(5) == "MONTHS")
      // add 3, TTL = 4weeks, so partition updated 29 days ago should be shown
      rows = spark.sql(s"call ttl_policy_save(table => '$tableName', spec => 'dt=202*', units => 'WEEKS', value => '4', level => 'PARTITION')").collect()
      assert(rows.length == 3)
      assert(rows(0).get(2) == "dt=202*")
      assert(rows(0).get(3) == "PARTITION")
      assert(rows(0).get(4) == "4")
      assert(rows(0).get(5) == "WEEKS")
      assert(rows(2).get(0) == "dt=2021-01-05")
      assert(rows(2).get(1) == ts)
      assert(rows(2).get(2) == "dt=202*")
      assert(rows(2).get(3) == "PARTITION")
      assert(rows(2).get(4) == "4")
      assert(rows(2).get(5) == "Weeks")

      val metaClient = HoodieTableMetaClient.builder().setBasePath(tablePath).setConf(spark.sessionState.newHadoopConf()).build()
      val ttlPolicyDAO = metaClient.getTtlPolicyDAO
      assert(ttlPolicyDAO.getAll.size() == 3)
      assert(ttlPolicyDAO.getAll.get(0).getUnits == ChronoUnit.MONTHS)
      assert(ttlPolicyDAO.getAll.get(0).getSpec == "*")
      assert(ttlPolicyDAO.getAll.get(1).getUnits == ChronoUnit.WEEKS)
      assert(ttlPolicyDAO.getAll.get(1).getSpec == "dt=202*")
      assert(ttlPolicyDAO.getAll.get(2).getUnits == ChronoUnit.DAYS)
      assert(ttlPolicyDAO.getAll.get(2).getSpec == "2023")
    }
  }

  test("Test positive Call ttl_policy_run") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${new Path(tmp.getCanonicalPath, tableName).toUri.toString}"
      createTable(tableName, tablePath)

      val timelineService = HoodieClientTestUtils.initTimelineService(new HoodieSparkEngineContext(spark.sparkContext), tablePath, 26754);

      // 3 policies
      var metaClient = HoodieTableMetaClient.builder().setBasePath(tablePath).setConf(spark.sessionState.newHadoopConf()).build()
      val ttlPolicyDAO = metaClient.getTtlPolicyDAO
      val policy1 = new TtlPolicy("*", TtlPolicyLevel.PARTITION, 5, ChronoUnit.DAYS)
      ttlPolicyDAO.save(policy1)
      val policy2: TtlPolicy = new TtlPolicy("dt=202*", TtlPolicyLevel.PARTITION, 4, ChronoUnit.WEEKS)
      ttlPolicyDAO.save(policy2)
      val policy3: TtlPolicy = new TtlPolicy("dt=2022-02-24", TtlPolicyLevel.PARTITION, 100, ChronoUnit.YEARS)
      ttlPolicyDAO.save(policy3)

      val ts1 = preparePartition(tableName, tablePath, "2021-12-03", 25L, 1)
      val ts2 = preparePartition(tableName, tablePath, "2019-01-05", 15L, 2)
      val ts3 = preparePartition(tableName, tablePath, "2022-02-24", 500L, 3)
      spark.sql(s"select id, name, price, ts, dt from $tableName ").show()

      // DRY-RUN, conflict resolution rule - default =  MAX_TTL
      var rows = spark.sql(s"call ttl_policy_run(table => '$tableName', dryRun => true)").collect()
      rows.foreach(r => println(r))
      assert(rows.length == 1)
      assert(rows(0).get(0) == "dt=2019-01-05")
      assert(rows(0).get(1) == ts2)
      assert(rows(0).get(2) == "*")
      assert(rows(0).get(3) == "PARTITION")
      assert(rows(0).get(4) == "5")
      assert(rows(0).get(5) == "Days")
      // as it was dry-run, partition is not deleted
      rows = spark.sql(s"select id, name, price, ts, dt from $tableName where dt = '2019-01-05' and id = 2").collect()
      rows.foreach(r => println(r))
      assert(rows.length == 1)

      // RUN, conflict resolution rule - MAX_TTL, NUM_COMMITS = 1
      TtlConfigurer.setAndGetTtlConf(metaClient.getFs, metaClient.getMetaPath, "true", null, "1", null)
      rows = spark.sql(s"call ttl_policy_run(path => '$tablePath', dryRun => false)").collect()
      rows.foreach(r => println(r))
      assert(rows.length == 1)
      // check results of manual TTL execution
      var activeTimeline = metaClient.getActiveTimeline
      var latestInstant = activeTimeline.filterCompletedInstants.lastInstant.orElse(null)
      var commitBytes = activeTimeline.getInstantDetails(latestInstant).get()
      var replaceCommitMetadata = HoodieReplaceCommitMetadata.fromBytes(commitBytes, classOf[HoodieReplaceCommitMetadata])
      println(replaceCommitMetadata)
      assertEquals(WriteOperationType.DELETE_PARTITION, replaceCommitMetadata.getOperationType)
      assertEquals(1, replaceCommitMetadata.getPartitionToReplaceFileIds.size)
      assertTrue(replaceCommitMetadata.getPartitionToReplaceFileIds().containsKey("dt=2019-01-05"))
      assertTrue(existsPath(tablePath + "/dt=2019-01-05")) // but partition directory still exists in filesystem

      // DRY-RUN, conflict resolution rule - MIN_TTL
      TtlConfigurer.setAndGetTtlConf(metaClient.getFs, metaClient.getMetaPath, null, null, null, TtlPoliciesConflictResolutionRule.MIN_TTL.name)
      rows = spark.sql(s"call ttl_policy_run(path => '$tablePath', dryRun => true)").collect()
      rows.foreach(r => println(r))
      assert(rows.length == 3) // 3 not 2, as partition dt=2019-01-05 still exists in fs

      // make commit, after this commit ttl_policy_runs inline!
      val ts4 = preparePartition(tableName, tablePath, "1985-04-26", 4L, 4)

      // check results of inline TTL execution
      metaClient.reloadActiveTimeline()
      activeTimeline = metaClient.getActiveTimeline
      latestInstant = activeTimeline.filterCompletedInstants.lastInstant.orElse(null)
      commitBytes = activeTimeline.getInstantDetails(latestInstant).get()
      replaceCommitMetadata = HoodieReplaceCommitMetadata.fromBytes(commitBytes, classOf[HoodieReplaceCommitMetadata])
      println(replaceCommitMetadata)
      assertEquals(WriteOperationType.DELETE_PARTITION, replaceCommitMetadata.getOperationType)
      assertEquals(3, replaceCommitMetadata.getPartitionToReplaceFileIds.size)
      assertTrue(replaceCommitMetadata.getPartitionToReplaceFileIds().containsKey("dt=2021-12-03"))
      assertTrue(replaceCommitMetadata.getPartitionToReplaceFileIds().containsKey("dt=2019-01-05"))
      assertTrue(replaceCommitMetadata.getPartitionToReplaceFileIds().containsKey("dt=2022-02-24"))

      // so only one record should remain
      spark.sql(s"select id, name, price, ts, dt from $tableName ").show()
      rows = spark.sql(s"select id, name, price, ts, dt from $tableName").collect()
      assert(rows.length == 1)
      assert(rows(0).get(0) == 4)
      assert(rows(0).get(4) == "1985-04-26")

      if (timelineService != null) {
        timelineService.close()
      }
    }
  }

  private def preparePartition(tableName: String, tablePath: String, partition: String, daysBefore: Long, id: Integer): String = {
    spark.sql(s"insert into $tableName values ($id, 'a$id', 10, 1000, '$partition')").collect()
    // create old partition metadata
    val metaClient = HoodieTableMetaClient.builder().setBasePath(tablePath).setConf(spark.sessionState.newHadoopConf()).build()
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
    val now = LocalDateTime.now
    val ts = HoodieInstantTimeGenerator.getInstantForDateString(now.minusDays(daysBefore).format(formatter))
    val writtenMetadata = new HoodiePartitionMetadata(metaClient.getFs, ts, new Path(tablePath), new Path(tablePath, "dt=" + partition), Option.empty())
    writtenMetadata.trySave(0)
    ts
  }

  test("Test negative Call ttl_policy_run") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${new Path(tmp.getCanonicalPath, tableName).toUri.toString}"
      createTable(tableName, tablePath)

      // ttl disabled, no policies
      var ex = intercept[InvalidTtlPolicyException] {
        spark.sql(s"call ttl_policy_run(path => '$tablePath', dryRun => true)").collect()
      }
      assertEquals("TTL policies are not configured", ex.getMessage)
      ex = intercept[InvalidTtlPolicyException] {
        spark.sql(s"call ttl_policy_run(path => '$tablePath', dryRun => false)").collect()
      }
      assertEquals("TTL policies are not configured", ex.getMessage)

      // ttl disabled, 2 policies
      val metaClient = HoodieTableMetaClient.builder().setBasePath(tablePath).setConf(spark.sessionState.newHadoopConf()).build()
      val ttlPolicyDAO = metaClient.getTtlPolicyDAO
      val policy1 = new TtlPolicy("*", TtlPolicyLevel.PARTITION, 12, ChronoUnit.MONTHS)
      var json = JsonUtils.toString(policy1)
      ttlPolicyDAO.save(json)
      val policy2: TtlPolicy = new TtlPolicy("2023", TtlPolicyLevel.PARTITION, 2, ChronoUnit.YEARS)
      json = JsonUtils.toString(policy2)
      ttlPolicyDAO.save(json)

      var rows = spark.sql(s"call ttl_policy_run(path => '$tablePath', dryRun => true)").collect()
      assert(rows.length == 0)
      rows = spark.sql(s"call ttl_policy_run(path => '$tablePath', dryRun => true)").collect()
      assert(rows.length == 0)
    }
  }

  private def createTable(tableName: String, tablePath: String) = {
    spark.sql(
      s"""
         |create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  ts long,
         |  dt string
         |) using hudi
         | location '$tablePath'
         | options (
         |  primaryKey ='id',
         |  type = 'cow',
         |  preCombineField = 'ts'
         | )
         | partitioned by (dt)
     """.stripMargin)
  }
}
