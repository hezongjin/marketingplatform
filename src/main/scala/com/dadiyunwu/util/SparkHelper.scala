package com.dadiyunwu.util

import java.sql.{Connection, DriverManager}
import java.time.LocalDate
import java.util.Properties

import com.dadiyunwu.comm.SparkConstants
import org.apache.spark.SparkConf
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable
import scala.io.Source

/**
  * spark 工具类
  */
case class JDBCConstant(table: String, columnName: String = null, lowerBound: Long = 0, upperBound: Long = 4, numPartitions: Int = 4)

object SparkHelper {


  def getSparkConf(name: String): SparkConf = {
    val conf = new SparkConf()
      .setMaster(SparkConstants.master)
      .setAppName(name)
      .set("dfs.client.socket-timeout", "700000")
      .set("hbase.rpc.timeout", "800000")
      .set("phoenix.query.timeoutMs", "900000")
      .set("hbase.client.scanner.caching", "100000")
      .set("spark.task.maxFailures", "8")
      .set("spark.shuffle.io.retryWait", "60s")
    //      .set("spark.sql.shuffle.partitions", "200")
    //      .set("spark.sql.warehouse.dir", "spark-warehouse")

    conf
  }

  def getSparkSession(conf: SparkConf): SparkSession = {
    val spark = SparkSession.builder().config(conf).getOrCreate()

    registerUDF(spark)

    spark.sparkContext.setLogLevel("WARN")
    spark
  }

  def getSparkSessionWithHive(conf: SparkConf): Unit = {
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    spark
  }

  /**
    * 注册UDF
    *
    * @param spark
    * @return
    */
  def registerUDF(spark: SparkSession): SparkSession = {

    val industryMap = CommHelper.readFile2Map4StringFromSpark(spark, "industry.txt")
    val regionMap = CommHelper.readFile2Map4StringFromSpark(spark, "region.txt")

    val getIndus = (key: String) => {
      var indus: String = ""
      try {
        //\u0002 hbase的分隔符
        val keys = key.split("\u0002")
        for (elem <- keys) {
          val value = industryMap(elem)
          if (value != null && (!value.equals(""))) {
            indus += industryMap(elem) + ","
          }
        }

        indus = indus.substring(0, indus.size - 1)

      } catch {
        case ex: Exception => {}
      }
      indus
    }

    val getRegion = (key: String) => {
      var region: String = ""
      try {
        region = regionMap(key)
      } catch {
        case e: Exception => {}
      }
      region
    }

    spark.udf.register("getIndus", getIndus)
    spark.udf.register("getRegion", getRegion)

    spark
  }

  /**
    * 读取phoenix中的表
    *
    * @param spark
    * @param table
    * @return
    */
  def getTableFromHbase(spark: SparkSession, table: String): DataFrame = {
    spark.read.format(SparkConstants.phoenix)
      .option(SparkConstants.table, table)
      .option(SparkConstants.zkUrl, SparkConstants.zkSer)
      .option("phoenix.query.timeoutMs", "100000")
      .option("hbase.client.scanner.timeout.period", "90000")
      .option("hbase.rpc.timeout", "80000")
      .load()
  }

  def getTableFromParquet(spark: SparkSession, table: String): DataFrame = {
    val df = spark.read.parquet(s"/ori/${table}")
    df
  }

  def getFilePath(fileName: String): String = {
    var path: String = null
    path = s"hdfs://bdpcluster/ori/map/${fileName}"
    /* if (SparkConstants.tag.equals("prod")) {
       path = s"hdfs://bdpcluster/ori/map/${fileName}"
     } else {
       path = SparkHelper.getClass.getClassLoader.getResource(fileName).getPath
     }*/
    path
  }

  def readDataFromCK(spark: SparkSession, table: String): Unit = {

    val prop = new Properties()
    prop.put("driver", classOf[ru.yandex.clickhouse.ClickHouseDriver].getName)
    prop.put("user", "default")
    prop.put("password", "c6hP16Fd")

    import org.apache.spark.sql.functions._

    val url = "jdbc:clickhouse://10.112.1.15:8123/ODS_LOCAL?useUnicode=true&characterEncoding=UTF-8"

    val start = System.currentTimeMillis()
    //    val table = "(select CUST_ID,(ABS(javaHash((CUST_ID))) % 9) as hash_id from DIM.DIM_CE17_CUST) temp"
    //    val table = "(select *,(ABS(javaHash((ID))) % 9) as hash_id from ODS.nrcp_order_info) temp"
    val table = "(select custId,custNameCn,createDate,(ABS(javaHash((custId))) % 9) as hash_id from ODS_CE17.moncma_cm_cust) data"

    val custBaseDF = spark.read.jdbc(url, table, "hash_id", 0, 9, 9, prop)

    val path = "hdfs://test-hdp1.novalocal:8020/data/part-r-00000-1e695f11-df0a-4a92-b287-51e05a873d1b.snappy.parquet"
    val custDF = spark.read.parquet(path).selectExpr("cust_id as custId")

    val table2 = "(select custId,entId,(ABS(javaHash((custId))) % 9) as hash_id from ODS_CE17.moncma_cm_cust_entid_rel ) data"

    val entDF = spark.read.jdbc(url, table2, "hash_id", 0, 9, 9, prop)


    val df = custDF.join(custBaseDF, custDF("custId") === custBaseDF("custId"), "left")
      .select(custDF("custId"), custBaseDF("custNameCn"), custBaseDF("createDate"))

    df.join(entDF, df("custId") === entDF("custId"), "left")
      .select(df("*"), entDF("entId"))
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet("/data/mango_data")


  }

  /**
    *
    * @param spark
    * @param useConcurrent 是否多线程拉取 true是 false否
    * @param jdbcConstant  拉取参数
    * @return
    */
  def getTableFromJDBC(spark: SparkSession, useConcurrent: Boolean, jdbcConstant: JDBCConstant): DataFrame = {

    val prop = new Properties()
    prop.put("driver", classOf[ru.yandex.clickhouse.ClickHouseDriver].getName)
    prop.put("user", SparkConstants.CLICKHOUSE_USER)
    prop.put("password", SparkConstants.CLICKHOUSE_PASSWORD)
    val url = SparkConstants.CLICKHOUSE_URL()

    var jdbcTable: DataFrame = null
    if (!useConcurrent) {
      jdbcTable = spark.read.jdbc(
        url,
        jdbcConstant.table,
        prop
      )
    } else {
      jdbcTable = spark.read.jdbc(
        url,
        jdbcConstant.table,
        jdbcConstant.columnName,
        jdbcConstant.lowerBound,
        jdbcConstant.upperBound,
        jdbcConstant.numPartitions,
        prop
      )

    }
    jdbcTable
  }

  def getDIMTable(spark: SparkSession): DataFrame = {
    val jdbcConstant = JDBCConstant("DIM.DIM_CE17_CUST")
    val DIMTable = SparkHelper.getTableFromJDBC(spark, false, jdbcConstant)
      .selectExpr("cust_id")
    DIMTable
  }

  def getEntTable(spark: SparkSession): DataFrame = {

    val sql =
      """
        |(
        |select
        |	custId ,
        |	entId ,
        |	createTime ,
        |	custName,
        |	abs(javaHash(custId))%4 as hashID
        |from
        |	(
        |	select
        |		custId ,
        |		entId,
        |		createTime ,
        |		custName,
        |		updateTime
        |	from
        |		ODS_CE17.moncma_cm_cust_entid_rel mccer
        |	where
        |		entId != ''
        |		and entId is not null )
        |order by
        |	updateTime
        |limit 1 by custId
        |) tmp
      """.stripMargin

    val constant = JDBCConstant(sql, "hashID")
    val entTable = this.getTableFromJDBC(spark, true, constant)

    entTable
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("as").setMaster("local[9]")
      .set("spark.sql.warehouse.dir", "E:/learn/marketingplatform/spark-warehouse")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val df = this.getEntTable(spark)
    df.show()

    spark.stop()

  }

}
