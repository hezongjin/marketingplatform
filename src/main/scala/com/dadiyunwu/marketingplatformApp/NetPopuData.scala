package com.dadiyunwu.marketingplatformApp

import com.dadiyunwu.comm.SparkConstants
import com.dadiyunwu.util.SparkHelper
import org.apache.spark.sql.functions._

/**
  * 计算推广信息关键词
  */
object NetPopuData {
  def main(args: Array[String]): Unit = {
    val conf = SparkHelper.getSparkConf(NetPopuData.getClass.getName)
    val spark = SparkHelper.getSparkSession(conf)
    spark.sparkContext.setLogLevel("WARN")

    //todo 使用SparkHelper中的方法 获取
    val sourceDF = spark.read.parquet("/ori/marketing")
      .where(col("entId").isNotNull)
      .selectExpr("entId as ent_id", "custId as cust_id", "createDate", "custNameCn")
      .repartition(20)

    val netPopuDF = SparkHelper.getTableFromHbase(spark, SparkConstants.T_EDS_ENT_NET_POPULARIZATION)
      .where(col("ent_id").isNotNull)

    sourceDF
      .join(netPopuDF, Seq("ent_id"), "left")
      .where(netPopuDF("ent_id").isNotNull)
      .select(
        sourceDF("ent_id"),
        netPopuDF("popu_keyword"), //聚合
        netPopuDF("popu_plat"),
        netPopuDF("popu_type"),
        netPopuDF("sem_url"),
        netPopuDF("MODIFY_TIME")
      )
      .createOrReplaceTempView("popu")

    //todo 更改字段名字
    val sinkDF = spark.sql(
      """
        |with t1 as (
        |select
        |	 ent_id,
        |	 popu_plat,
        |	 popu_type,
        |	 sem_url,
        |	 popu_keyword,
        |	 Date(modify_time) as modify_time,
        |	 rank() over( partition by ent_id,popu_plat,popu_type,sem_url order by Date(modify_time) desc) as rk
        |from popu
        |)
        |select
        |  ent_id,
        |  popu_plat,
        |  popu_type,
        |  sem_url,
        |  modify_time,
        |  collect_set(popu_keyword) as popu_keywords
        |from t1
        |where rk=1
        |group by
        |  ent_id,
        |  popu_plat,
        |  popu_type,
        |  sem_url,
        |  modify_time
      """.stripMargin)

    sinkDF
      .repartition(1)
      .write
      .format("parquet")
      .save("/ori/testmarketing")

    spark.sparkContext.stop()
    spark.stop()

  }
}
