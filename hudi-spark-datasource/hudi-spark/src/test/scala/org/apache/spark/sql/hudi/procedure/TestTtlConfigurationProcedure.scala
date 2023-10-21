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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.common.table.ttl.model.{TtlPoliciesConflictResolutionRule, TtlTriggerStrategy}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.exception.InvalidTtlPolicyException
import org.apache.spark.sql.hudi.HoodieSparkSqlTestBase
import org.junit.jupiter.api.Assertions.{assertFalse, assertTrue}

class TestTtlConfigurationProcedure extends HoodieSparkSqlTestBase {

  test("Test Call ttl_configuration show defaults") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${new Path(tmp.getCanonicalPath, tableName).toUri.toString}"
      createTable(tableName, tablePath)

      var rows = spark.sql(s"call ttl_configuration(table => '$tableName')").collect()
      rows.foreach(r => println(r))
      assert(rows.length == 4)

      rows = spark.sql(s"call ttl_configuration(path => '$tablePath')").collect()
      assert(rows.length == 4)
    }
  }

  test("Test Call ttl_configuration edit success") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${new Path(tmp.getCanonicalPath, tableName).toUri.toString}"
      createTable(tableName, tablePath)

      // ttl turn ON
      var rows = spark.sql(s"call ttl_configuration(table => '$tableName', enabled => 'true')").collect()
      assert(rows.length == 1)
      assert(rows(0).toString() == "[hoodie.ttl.policies.enabled,true]")
      var config = getTableConfig(tablePath)
      assertTrue(config.isTtlPoliciesEnabled)

      // change only value
      rows = spark.sql(s"call ttl_configuration(table => '$tableName', value => '50')").collect()
      assert(rows.length == 1)
      assert(rows(0).toString() == "[hoodie.ttl.policies.trigger.value,50]")
      config = getTableConfig(tablePath)
      assertTrue(config.isTtlPoliciesEnabled)
      assert(config.getTtlTriggerValue == "50")
      assert(config.getTtlTriggerStrategy == TtlTriggerStrategy.NUM_COMMITS.toString)
      assert(config.getTtlPoliciesConflictResolutionRule == TtlPoliciesConflictResolutionRule.MAX_TTL.toString)

      // turn OFF and change strategy
      rows = spark.sql(s"call ttl_configuration(table => '$tableName', enabled => 'false', strategy => 'TIME_ELAPSED')").collect()
      assert(rows.length == 2)
      assert(rows(0).toString() == "[hoodie.ttl.policies.enabled,false]")
      assert(rows(1).toString() == "[hoodie.ttl.policies.trigger.strategy,TIME_ELAPSED]")
      config = getTableConfig(tablePath)
      assertFalse(config.isTtlPoliciesEnabled)
      assert(config.getTtlTriggerStrategy == TtlTriggerStrategy.TIME_ELAPSED.toString)
      assert(config.getTtlTriggerValue == "50")

      // turn ON with strategy, value and conflictResolutionRule settings
      rows = spark.sql(s"call ttl_configuration(table => '$tableName', enabled => 'true', strategy => 'NUM_COMMITS', value => '8', resolveConflictsBy => 'MIN_TTL')").collect()
      assert(rows.length == 4)
      assert(rows(0).toString() == "[hoodie.ttl.policies.conflict.resolution.rule,MIN_TTL]")
      assert(rows(1).toString() == "[hoodie.ttl.policies.enabled,true]")
      assert(rows(2).toString() == "[hoodie.ttl.policies.trigger.strategy,NUM_COMMITS]")
      assert(rows(3).toString() == "[hoodie.ttl.policies.trigger.value,8]")
      config = getTableConfig(tablePath)
      assertTrue(config.isTtlPoliciesEnabled)
      assert(config.getTtlTriggerStrategy == TtlTriggerStrategy.NUM_COMMITS.toString)
      assert(config.getTtlTriggerValue == "8")
      assert(config.getTtlPoliciesConflictResolutionRule == TtlPoliciesConflictResolutionRule.MIN_TTL.toString)

      // show configuration
      rows = spark.sql(s"call ttl_configuration(path => '$tablePath')").collect()
      assert(rows.length == 4)
      assert(rows(0).toString() == "[hoodie.ttl.policies.conflict.resolution.rule,MIN_TTL]")
      assert(rows(1).toString() == "[hoodie.ttl.policies.enabled,true]")
      assert(rows(2).toString() == "[hoodie.ttl.policies.trigger.strategy,NUM_COMMITS]")
      assert(rows(3).toString() == "[hoodie.ttl.policies.trigger.value,8]")
    }
  }

  test("Test Call ttl_configuration edit failure") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${new Path(tmp.getCanonicalPath, tableName).toUri.toString}"
      createTable(tableName, tablePath)

      // invalid enabled
      val rows = spark.sql(s"call ttl_configuration(table => '$tableName', enabled => 'invalid')").collect()
      assert(rows.length == 1)
      assert(rows(0).toString() == "[hoodie.ttl.policies.enabled,false]")

      // invalid strategy
      assertThrows[InvalidTtlPolicyException] {
        spark.sql(s"call ttl_configuration(table => '$tableName', strategy => 'INVALID_STRATEGY')").collect()
      }

      // invalid value
      assertThrows[InvalidTtlPolicyException] {
        spark.sql(s"call ttl_configuration(table => '$tableName', value => '<=8')").collect()
      }
      assertThrows[InvalidTtlPolicyException] {
        spark.sql(s"call ttl_configuration(table => '$tableName', value => '-8')").collect()
      }
      assertThrows[InvalidTtlPolicyException] {
        spark.sql(s"call ttl_configuration(table => '$tableName', value => '0')").collect()
      }
      // invalid conflict resolution rule
      assertThrows[InvalidTtlPolicyException] {
        spark.sql(s"call ttl_configuration(table => '$tableName', resolveConflictsBy => 'INVALID_RULE')").collect()
      }

      val config = getTableConfig(tablePath)
      assertFalse(config.isTtlPoliciesEnabled)
      assert(config.getTtlTriggerStrategy == TtlTriggerStrategy.NUM_COMMITS.toString)
      assert(config.getTtlTriggerValue == "10")
      assert(config.getTtlPoliciesConflictResolutionRule == TtlPoliciesConflictResolutionRule.MAX_TTL.toString)
    }
  }

  private def createTable(tableName: String, tablePath: String) = {
    spark.sql(
      s"""
         |create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  ts long
         |) using hudi
         | location '$tablePath'
         | options (
         |  primaryKey ='id',
         |  type = 'cow',
         |  preCombineField = 'ts'
         | )
         | partitioned by(ts)
     """.stripMargin)
  }

  private def getTableConfig(tablePath: String): HoodieTableConfig = {
    val metaPath = tablePath + '/' + HoodieTableMetaClient.METAFOLDER_NAME
    new HoodieTableConfig(new Path("/").getFileSystem(new Configuration), metaPath, null, null)
  }
}
