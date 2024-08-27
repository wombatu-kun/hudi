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

package org.apache.spark.sql.hudi.dml

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

class TestSparkBUGS extends HoodieSparkSqlTestBase {

  test("Test writing null to the hudi table [BUG2024081405323]") {
    withTempDir { tmp =>
        spark.sql("set hoodie.schema.on.read.enable=false")
        val tbParquet = "tb_parquet"
        spark.sql(
          s"""
             |create table $tbParquet (
             |  id int, comb int,
             |  col0 int, col1 bigint, col2 float, col3 double, col4 decimal(10,4), col5 string, col6 date, col7 timestamp,
             |  col8 boolean, col9 binary, par date
             |) using parquet
             |location '${tmp.getCanonicalPath}/$tbParquet'
             |""".stripMargin)
        spark.sql(
          s"""
            |insert into $tbParquet values
            |(1,1,11,100001,101.01,1001.0001,100001.0001,'a000001',CAST('2021-12-25' AS DATE),CAST('2021-12-25 12:01:01' AS TIMESTAMP),true,CAST('a01' AS BINARY),CAST('2021-12-25' AS DATE)),
            |(2,2,12,100002,102.02,1002.0002,100002.0002,'a000002',CAST('2021-12-25' AS DATE),CAST('2021-12-25 12:02:02' AS TIMESTAMP),true,CAST('a02' AS BINARY),CAST('2021-12-25' AS DATE)),
            |(3,3,13,100003,103.03,1003.0003,100003.0003,'a000003',CAST('2021-12-25' AS DATE),CAST('2021-12-25 12:03:03' AS TIMESTAMP),false,CAST('a03' AS BINARY),CAST('2021-12-25' AS DATE)),
            |(4,4,14,100004,104.04,1004.0004,100004.0004,'a000004',CAST('2021-12-26' AS DATE),CAST('2021-12-26 12:04:04' AS TIMESTAMP),true,CAST('a04' AS BINARY),CAST('2021-12-26' AS DATE)),
            |(5,5,15,100005,105.05,1005.0005,100005.0005,'a000005',CAST('2021-12-26' AS DATE),CAST('2021-12-26 12:05:05' AS TIMESTAMP),false,CAST('a05' AS BINARY),CAST('2021-12-26' AS DATE))
            |""".stripMargin)

        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             | id int, comb int,
             | col0 int default 0, col1 bigint default 10000, col2 float default 100.001, col3 double default 10000.00001,
             | col4 decimal(10,4) default 100000.0001, col5 string default 'DEFAULT默认值', col6 date default '2000-01-01',
             | col7 timestamp default '2000-01-01 00:00:00', par date
             |) using hudi
             | partitioned by(par)
             | tblproperties(
             |   type='cow',
             |   primaryKey='id',
             |   preCombineField='comb'
             | )
             | location '${tmp.getCanonicalPath}/$tableName'
       """.stripMargin)

      //val metaClient = createMetaClient(spark, s"${tmp.getCanonicalPath}/$tableName")
      val list = spark.sql(s"desc formatted $tableName").collectAsList()
      list.forEach(x => println(x))
        //spark.sql(s"insert into $tableName (id,comb,col2,par) select id,comb,col2,par from $tbParquet where id < 3")
        //spark.sql(s"alter table $tableName add columns(col8 boolean default 'false' after col7, col9 binary)")
        //spark.sql(s"insert into $tableName (id,comb,col2,col9,par) select id,comb,col2,col9,par from $tbParquet where id = 3")
        //spark.sql(s"alter table $tableName alter column col9 set default CAST('A00' AS BINARY)")
        //spark.sql(s"insert into $tableName (id,comb,col2, par) select id,comb,null,par from $tbParquet where id = 4")
        //spark.sql(s"select * from $tableName").show()

    }
  }

  test("Test describe table") {
    withTempDir { tmp =>
      val tbName = "wk_date"
      val basePath = s"${tmp.getCanonicalPath}/$tbName"

      spark.sql(
        s"""
           |create table $tbName (id int, driver string, precomb int, dat string)
           | using hudi
           | partitioned by(dat)
           | tblproperties(type='cow', primaryKey='id', preCombineField='precomb')
           | location '$basePath'
       """.stripMargin)

      spark.sql("set hoodie.schema.on.read.enable=false")
      spark.sql(s"desc formatted $tbName").show(100)

      spark.sql("set hoodie.schema.on.read.enable=true")
      spark.sql(s"desc formatted $tbName").show(100)
    }
  }

  test("Test partition incomplete [BUG2024081502744]") {
    withTempDir { tmp =>
      val tbName = "wk_date"
      spark.sql(s"""
        |create table $tbName (keyid int, dat string, col0 double, col1 string, col2 date)
        | using parquet
        | location '${tmp.getCanonicalPath}/$tbName'
        """.stripMargin)
      spark.sql(s"""
           |insert into $tbName values
           | (1001,'2021/01/01',10.202,'aaa',cast('2021-01-01' as date)),
           | (1002,'2021/02/02',20.303,'bbb',cast('2021-02-02' as date)),
           | (1003,'2021/03/03',30.404,'ccc',cast('2021-03-03' as date))
           """.stripMargin)

      val dateDf = spark.sql(s"select * from $tbName")
      println("df-Show:")
      dateDf.show()

      dateDf.write.format("hudi")
        .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
        .option("hoodie.datasource.write.precombine.field", "col2")
        .option("hoodie.datasource.write.recordkey.field", "keyid")
        .option("hoodie.datasource.write.partitionpath.field", "dat")
        .option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.SimpleKeyGenerator")
        .option("hoodie.datasource.write.operation", "bulk_insert")
        //.option("hoodie.datasource.hive_sync.enable", "true") // orig
        .option("hoodie.datasource.hive_sync.partition_fields", "dat")
        .option("hoodie.datasource.hive_sync.partition_extractor_class", "org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor")
        .option("hoodie.datasource.hive_sync.database", "testdb")
        .option("hoodie.datasource.hive_sync.table", "tb_test_cow_date_par")
        .option("hoodie.datasource.hive_sync.use_jdbc", "false")
        .option("hoodie.bulkinsert.shuffle.parallelism", 4)
        .option("hoodie.upsert.shuffle.parallelism", 4)
        .option("hoodie.insert.shuffle.parallelism", 4)
        .option("hoodie.delete.shuffle.parallelism", 4)
        .option("hoodie.datasource.write.hive_style_partitioning", "true")
        .option("hoodie.table.name", "tb_test_cow_date_par")
        .mode(org.apache.spark.sql.SaveMode.Overwrite)
        .save(s"/tmp/testdb/tb_test_cow_date_par")

      spark.read.format("hudi").load("/tmp/testdb/tb_test_cow_date_par/*/*/*").orderBy("keyid").show(false)
    }
  }

  test("Simplest partition incomplete [BUG2024081502744]") {
    withTempDir { tmp =>
      val tbName = "wk_date"
      val basePath = s"$tmp/$tbName"
      val columns = Seq("id","driver","precomb","dat")
      val data = Seq((1,"driver-A",6,"2021/01/01"), (2,"driver-B",7,"2021/01/02"), (3,"driver-C",8,"2021/03/01"))

      val inserts = spark.createDataFrame(data).toDF(columns:_*)

      val hudi_options = Map(
        "hoodie.table.name" -> tbName,
        "hoodie.datasource.write.table.type"-> "COPY_ON_WRITE",
        "hoodie.datasource.write.recordkey.field" -> "id",
        "hoodie.datasource.write.precombine.field" -> "precomb",
        "hoodie.datasource.write.partitionpath.field" -> "dat",
        "hoodie.datasource.write.hive_style_partitioning" -> "true"
        )
      inserts.write.format("hudi")
        .options(hudi_options)
        .mode(org.apache.spark.sql.SaveMode.Overwrite)
        .save(basePath)

      val df = spark.read.format("hudi")
        //.option("hoodie.datasource.read.paths", s"$basePath/dat=2021/01/01,$basePath/dat=2021/01/02")
        .load(s"$basePath/*/*/*")

      df.select((Seq("_hoodie_partition_path") ++ columns).map(c => col(c)):_*).show()
    }
  }

  test("Simplified Test partition incomplete [BUG2024081502744]") {
    withTempDir { tmp =>
      val tbName = "wk_date"
      spark.sql(s"""
                   |create table $tbName (keyid int, dat string, col0 double, col1 string, col2 date)
                   | using parquet
                   | location '${tmp.getCanonicalPath}/$tbName'
        """.stripMargin)
      spark.sql(s"""
                   |insert into $tbName values
                   | (1001,'2021/01/01',10.202,'aaa',cast('2021-01-01' as date)),
                   | (1002,'2021/02/02',20.303,'bbb',cast('2021-02-02' as date)),
                   | (1003,'2021/03/03',30.404,'ccc',cast('2021-03-03' as date))
           """.stripMargin)

      val dateDf = spark.sql(s"select * from $tbName")
      dateDf.show()

      dateDf.write.format("hudi")
        .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
        .option("hoodie.datasource.write.precombine.field", "col2")
        .option("hoodie.datasource.write.recordkey.field", "keyid")
        .option("hoodie.datasource.write.partitionpath.field", "dat")
        .option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.SimpleKeyGenerator")
        .option("hoodie.datasource.write.hive_style_partitioning", "true")
        .option("hoodie.table.name", "tb_test_cow_date_par")
        .mode(org.apache.spark.sql.SaveMode.Overwrite)
        .save(s"/tmp/testdb/tb_test_cow_date_par")

      spark.read.format("hudi").load("/tmp/testdb/tb_test_cow_date_par/*/*/*").orderBy("keyid").show(false)
    }
  }
}
