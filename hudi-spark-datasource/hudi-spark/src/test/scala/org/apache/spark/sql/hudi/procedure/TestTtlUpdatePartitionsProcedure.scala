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
import org.apache.hudi.common.model.HoodiePartitionMetadata
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.spark.sql.hudi.HoodieSparkSqlTestBase
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}

import java.util.Properties

class TestTtlUpdatePartitionsProcedure extends HoodieSparkSqlTestBase {

  test("Test Call ttl_update_partitions") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${new Path(tmp.getCanonicalPath, tableName).toUri.toString}"
      createTable(tableName, tablePath)

      // empty timeline
      var rows = spark.sql(s"call ttl_update_partitions(table => '$tableName', dryRun => true)").collectAsList()
      assertEquals(0, rows.size())

      // add 3 old partitions (without lastUpdateTime) and 2 new
      addOldPartition(tableName, tablePath, "2019-10-05", 1)
      addOldPartition(tableName, tablePath, "2020-10-05", 2)
      addOldPartition(tableName, tablePath, "2021-10-05", 3)
      spark.sql(s"insert into $tableName values (4, 'a4', 10, 1000, '2022-10-05'),"
        + " (5, 'a5', 10, 1000, '2023-10-05')").collect()

      // DRY-run
      rows = spark.sql(s"call ttl_update_partitions(table => '$tableName', dryRun => true)").collectAsList()
      rows.forEach(r => println(r))
      assertEquals(5, rows.size)
      assertEquals(2, rows.stream().filter(r => r.get(1).toString.startsWith("already updated")).count())

      // update data in partition dt=2021-10-05
      spark.sql(s"update $tableName set price = 50 where id = 3").collect()
      // DRY-run and check one more "already updated"
      rows = spark.sql(s"call ttl_update_partitions(table => '$tableName', dryRun => true)").collectAsList()
      rows.forEach(r => println(r))
      assertEquals(5, rows.size)
      assertEquals(3, rows.stream().filter(r => r.get(1).toString.startsWith("already updated")).count())

      val metaClient = HoodieTableMetaClient.builder().setBasePath(tablePath).setConf(spark.sessionState.newHadoopConf()).build()

      // delete partition dt=2020-10-05
      metaClient.getFs.delete(new Path(tablePath, "dt=2020-10-05"), true)

      // RUN and check lastUpdateTime was written to all existing partitions
      rows = spark.sql(s"call ttl_update_partitions(table => '$tableName', dryRun => false)").collectAsList()
      assertEquals(5, rows.size())
      rows.forEach(r => {
        println(r)
        if (r.get(0).toString == "dt=2020-10-05") {
          assertEquals("does not exist", r.get(1).toString)
        } else {
          val metadata = new HoodiePartitionMetadata(metaClient.getFs, new Path(tablePath, r.get(0).toString))
          assertTrue(metadata.readPartitionUpdatedCommitTime().isPresent)
        }
      })

      // RUN second time and check everything is already updated
      rows = spark.sql(s"call ttl_update_partitions(table => '$tableName', dryRun => false)").collectAsList()
      assertEquals(5, rows.size())
      assertEquals(4, rows.stream().filter(r => r.get(1).toString.startsWith("already updated")).count())
    }
  }

  private def addOldPartition(tableName: String, tablePath: String, partition: String, id: Integer) {
    spark.sql(s"insert into $tableName values ($id, 'a$id', 10, 1000, '$partition')").collect()
    val metaClient = HoodieTableMetaClient.builder().setBasePath(tablePath).setConf(spark.sessionState.newHadoopConf()).build()
    val metadata = new HoodiePartitionMetadata(metaClient.getFs, new Path(tablePath, "dt=" + partition))
    val ts = metadata.readPartitionCreatedCommitTime().get()
    val depth = metadata.getPartitionDepth
    val metadataFilePath = new Path(tablePath + "/dt=" + partition, ".hoodie_partition_metadata")
    // delete current partition metadata
    metaClient.getFs.delete(metadataFilePath)
    // create old partition metadata without lastUpdateTime
    val props = new Properties
    props.setProperty(HoodiePartitionMetadata.COMMIT_TIME_KEY, ts)
    props.setProperty("partitionDepth", depth.toString)
    val os = metaClient.getFs.create(metadataFilePath, true)
    props.store(os, "partition metadata")
    os.hsync()
    os.hflush()
    os.close()
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
