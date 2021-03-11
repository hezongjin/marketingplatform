package com.dadiyunwu.util

import com.dadiyunwu.comm.SparkConstants
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

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
      .set("spark.sql.shuffle.partitions", "400")
      .set("spark.sql.warehouse.dir", "spark-warehouse")

    conf
  }

  def getSparkSession(conf: SparkConf): SparkSession = {
    val spark = SparkSession.builder().config(conf).getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark
  }

  def getSparkSessionWithHive(conf: SparkConf): SparkSession = {
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    spark
  }

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

}
