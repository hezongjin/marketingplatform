package com.dadiyunwu.marketingplatformApp

import java.net.URI
import java.time.LocalDate
import java.util.Properties

import com.dadiyunwu.comm.SparkConstants
import com.dadiyunwu.util.{ClickHouseHelper, SparkHelper}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/**
  * 计算推广信息关键词
  */
/*case class data(ent_id: String, cust_id: String, popu_keyword: String, popu_plat: String, popu_type: String,
                sem_url: String, MODIFY_TIME: String)*/

object NetPopuData {
  def main(args: Array[String]): Unit = {

    //添加重跑逻辑 删除今天的数据
    val connection = ClickHouseHelper.getCKConnection()
    val sql = "alter table ODS_CE17_LOCAL.ce17_hbase_tag_sem on cluster ch_cluster delete where DATA_DATE=(today()-1)";
    ClickHouseHelper.executeSql(connection, sql)
    ClickHouseHelper.closeCKConnection(connection)
    Thread.sleep(10 * 1000)

    val conf = SparkHelper.getSparkConf(NetPopuData.getClass.getName)
    val spark = SparkHelper.getSparkSession(conf)
    spark.sparkContext.setLogLevel("WARN")


    init(spark)
    val sourceDF = spark.read.parquet("/market/sourceInfo")

    val netPopuDF = spark.read.parquet("/market/SemInfo")

    //    val netPopuDF = spark.read.parquet("/ori/netPopu")
    //    val value: Broadcast[DataFrame] = spark.sparkContext.broadcast(entDF)

    /*  val sourceArray = sourceDF.rdd.mapPartitions(iter => {
        val arr = new ArrayBuffer[(String, Row)]()
        iter.foreach(it => {
          val cust_id = it.getAs[String]("cust_id")
          arr += ((cust_id, it))
        })
        arr.iterator
      }).collect()

      val sourceArrayBroad = spark.sparkContext.broadcast(sourceArray)


      val joinRDD = netPopuDF.rdd.mapPartitions(iter => {
        val arr = new ArrayBuffer[data]()
        val sourceArrayValue = sourceArrayBroad.value
        iter.foreach(it => {
          val netEntId = it.getAs[String]("ENT_ID")
          for (elem <- sourceArrayValue) {
            val sourceEntID = elem._2.getAs[String]("ent_id")

            if (sourceEntID == netEntId) {
              val ent_id = sourceEntID
              val cust_id = elem._1
              val popu_keyword = it.getAs[String]("POPU_KEYWORD")
              val popu_plat = it.getAs[String]("POPU_PLAT")
              val popu_type = it.getAs[String]("POPU_TYPE")
              val sem_url = it.getAs[String]("SEM_URL")
              val MODIFY_TIME = it.getAs[String]("MODIFY_TIME")
              val datacase = data(ent_id, cust_id, popu_keyword, popu_plat, popu_type, sem_url, MODIFY_TIME)
              arr += datacase
            }
          }
        })
        arr.iterator
      })
      import spark.implicits._
      val joinDF = joinRDD.toDF()

      println(joinDF.count())
      joinDF.show()*/


    val joinDF = sourceDF
      .join(netPopuDF, Seq("ent_id"), "left")
      .where(netPopuDF("ent_id").isNotNull)
      .select(
        sourceDF("ent_id"),
        sourceDF("cust_id"),
        netPopuDF("popu_keyword"), //聚合
        netPopuDF("popu_plat"),
        netPopuDF("popu_type"),
        netPopuDF("sem_url"),
        netPopuDF("MODIFY_TIME")
      )

    joinDF.createOrReplaceTempView("popu")

    val yesterday = LocalDate.now().plusDays(-1)
    val sinkDF = spark.sql(
      s"""
         |with t1 as (
         |select
         |	 cust_id,
         |	 popu_plat,
         |	 popu_type,
         |	 sem_url,
         |	 popu_keyword,
         |	 Date(modify_time) as modify_time,
         |	 rank() over( partition by cust_id order by Date(modify_time) desc) as rk
         |from popu
         |)
         |select
         |  cust_id as CUST_ID,
         |  popu_plat as POPU_PLAT,
         |  popu_type as POPU_TYPE,
         |  sem_url as SEM_URL,
         |  modify_time as RELEASE_TIME,
         |  concat_ws(',',collect_set(popu_keyword)) as POPU_KEYWORD_LIST
         |from t1
         |where rk=1
         |group by
         |  cust_id,
         |  popu_plat,
         |  popu_type,
         |  sem_url,
         |  modify_time
        """.stripMargin)

    val url = SparkConstants.CLICKHOUSE_URL()
    val table = "ODS_CE17.ce17_hbase_tag_sem"
    val prop = new Properties()
    prop.put("driver", SparkConstants.CLICKHOUSE_DERIVER)
    prop.put("user", SparkConstants.CLICKHOUSE_USER)
    prop.put("password", SparkConstants.CLICKHOUSE_PASSWORD)

    sinkDF
      .withColumn("DATA_DATE", lit(s"${yesterday}"))
      .write
      .mode(SaveMode.Append)
      .jdbc(url, table, prop)

    spark.sparkContext.stop()
    spark.stop()

  }

  def init(spark: SparkSession): Unit = {

    val hadoopConf = new Configuration()
    val fs = FileSystem.newInstance(new URI("hdfs://bdpcluster/"), hadoopConf)

    if (fs.exists(new Path("hdfs://bdpcluster//market/sourceInfo"))) {
      fs.delete(new Path("hdfs://bdpcluster//market/sourceInfo"), true)
    }

    val dimDF = SparkHelper.getDIMTable(spark)
    val entDF = SparkHelper.getEntTable(spark)
    val sourceDF = dimDF.join(entDF, dimDF("CUST_ID") === entDF("custId"), "left")
      .where(entDF("entId").isNotNull and entDF("custId").isNotNull)
      .select(
        dimDF("CUST_ID").alias("cust_id"),
        entDF("entId").alias("ent_id"),
        entDF("createTime").alias("createDate"),
        entDF("custName").alias("custNameCn")
      )
      .repartition(200)

    sourceDF.write.parquet("/market/sourceInfo")

    if (fs.exists(new Path("hdfs://bdpcluster//market/SemInfo"))) {
      fs.delete(new Path("hdfs://bdpcluster//market/SemInfo"), true)
    }

    val netPopuDF = SparkHelper.getTableFromHbase(spark, SparkConstants.T_EDS_ENT_NET_POPULARIZATION)
      .where(col("ent_id").isNotNull)
    netPopuDF.write.parquet("/market/SemInfo")


  }
}
