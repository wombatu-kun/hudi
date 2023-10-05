/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.procedure

import org.apache.hadoop.fs.Path
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.model.HoodiePartitionMetadata
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator
import org.apache.hudi.common.util.Option
import org.apache.hudi.exception.HoodieRollbackException
import org.apache.hudi.testutils.HoodieClientTestUtils
import org.apache.log4j.LogManager
import org.apache.spark.sql.hudi.HoodieSparkSqlTestBase
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotEquals}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class TestTtlRollback extends HoodieSparkSqlTestBase {

  private val log = LogManager.getLogger(getClass)
  private val tableName = "testTtlRollback"

  test("testRollbackPartitionTtl") {
    withTempDir { tmpFile =>
      // create table and prepare meta client
      val tablePath = new Path(tmpFile.getCanonicalPath, tableName).toUri.toString
      createTable(tableName, tablePath)
      val metaClient = HoodieTableMetaClient.builder().setBasePath(tablePath).setConf(spark.sessionState.newHadoopConf()).build()
      val timelineService = HoodieClientTestUtils.initTimelineService(new HoodieSparkEngineContext(spark.sparkContext), tablePath, 26754);

      // set one policy for all partitions
      spark.sql(s"call ttl_configuration(table => '$tableName', enabled => 'true')")
      spark.sql(s"call ttl_policy_save(table => '$tableName', spec => '*', units => 'YEARS', value => '1', level => 'PARTITION')")

      // prepare two partitions: new and outdated, with one record in each partition
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
      val now = LocalDateTime.now
      val currentDate = now.minusDays(0L).format(formatter)
      val historyDate = now.minusDays(370L).format(formatter)
      val currentYear = currentDate.substring(0, 4)
      val historyYear = historyDate.substring(0, 4)

      preparePartitions(metaClient, tableName, tablePath, historyYear, historyDate, 1)
      preparePartitions(metaClient, tableName, tablePath, currentYear, currentDate, 2)
      var records = spark.sql(s"select id, preComb, year from $tableName").collectAsList()
      log.info("Records before TTL running:")
      records.forEach(row => log.info(row.toString()))
      assertEquals(2, records.size(), "Before TTL running there should be all records in query results")

      // Run partition TTL
      val oldPartitions = spark.sql(s"call ttl_policy_run(table => '$tableName', dryRun => false)").collectAsList()
      log.info("Partitions for removing:")
      oldPartitions.forEach(oldPartition => log.info(oldPartition))
      assertEquals(1, oldPartitions.size(), "There should be only one outdated partition")
      assertEquals(s"year=$historyYear", oldPartitions.get(0).get(0), s"Only 'year=$historyYear' partition should be outdated. Note that we expect partition as a first element in the row")

      records = spark.sql(s"select id, preComb, year from $tableName").collectAsList()

      // Additional commit after replacecommit
      preparePartitions(metaClient, tableName, tablePath, currentYear, currentDate, 3)
      records = spark.sql(s"select id, preComb, year from $tableName").collectAsList()
      log.info("Records with additional commit after TTL running:")
      records.forEach(row => log.info(row.toString()))
      assertEquals(2, records.size(), "Commit after TTL running would increase number of rows to 2")
      records.forEach(row => assertNotEquals(s"$historyYear", row.get(2), s"There shouldn't be outdated year, $historyYear, in query results"))

      // Rollbacks
      // Commits are in a reverse order: 0 is the last commit, 1 is the replacecommit.
      val commits = spark.sql(s"call show_commits(table => '$tableName', limit => 10)").collect()
      // Bad naming, instant_time means in rollback_to_instant that commit with this instant time would be rolled back
      // try to rollback two last commits simultaneously
      val ex = intercept[HoodieRollbackException] {
        spark.sql(s"call rollback_to_instant(table => '$tableName', instant_time => '${commits(1).get(0).toString}')")
      }
      assertEquals(s"Found commits after time :${commits(1).get(0).toString}, please rollback greater commits first", ex.getCause.getMessage)

      // rollback two last commits one by one
      spark.sql(s"call rollback_to_instant(table => '$tableName', instant_time => '${commits(0).get(0).toString}')")
      records = spark.sql(s"select id, preComb, year from $tableName").collectAsList()
      assertEquals(1, records.size(), "After rollback of the last commit, we expect only one record")
      // target rollback of the commit
      spark.sql(s"call rollback_to_instant(table => '$tableName', instant_time => '${commits(1).get(0).toString}')")
      records = spark.sql(s"select id, preComb, year from $tableName").collectAsList()
      assertEquals(2, records.size(), "After rollback of the replacecommit, we expect two records again")
      records.forEach { row =>
        if (row.get(0) == 2) {
          assertEquals(s"$currentYear", row.get(2), s"For ID=2 we expect that current year, $currentYear, is still presented in query results")
        } else {
          assertEquals(s"$historyYear", row.get(2), s"We expect that outdated year, $historyYear, is restored in query results")
        }
      }

      if (timelineService != null) {
        timelineService.close()
      }
    }
  }

  private def createTable(tableName: String, tablePath: String): Unit = {
    // required fields: id, preComb; additionally we need 'year' for partitioning
    spark.sql(
      s"""
         |create table $tableName (
         |  id int,
         |  preComb long,
         |  year string
         |) using hudi
         | location '$tablePath'
         | options (
         |  primaryKey ='id',
         |  type = 'cow',
         |  preCombineField = 'preComb'
         | )
         | partitioned by (year)
      """.stripMargin)
    log.info(s"Table $tableName is created at $tablePath")
  }

  private def preparePartitions(metaClient: HoodieTableMetaClient, tableName: String, tablePath: String, partition: String, timestamp: String, id: Integer): Unit = {
    spark.sql(s"insert into $tableName values ($id, 1000, '$partition')").collect()
    val writtenMetadata = new HoodiePartitionMetadata(metaClient.getFs, HoodieInstantTimeGenerator.getInstantForDateString(timestamp),
      new Path(tablePath), new Path(tablePath, s"year=$partition"), Option.empty())
    writtenMetadata.trySave(0)
  }
}
