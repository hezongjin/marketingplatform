package com.dadiyunwu.marketingplatformApp

import com.dadiyunwu.comm.SparkConstants
import com.dadiyunwu.util.{ColumnHelper, SparkHelper}
import org.apache.spark.sql.functions._

object load {

  private val ent_id = "ent_id"

  def main(args: Array[String]): Unit = {

    val conf = SparkHelper.getSparkConf(SparkConstants.name)
    //      .set("spark.sql.warehouse.dir", "spark-warehouse")

    val spark = SparkHelper.getSparkSession(conf)
    val entId = ColumnHelper.getEntID()

    /* //商标 31697925
     val treadMarkDF = SparkHelper.getTableFromHbase(spark, SparkConstants.T_EDS_ENT_TRADEMARK_INFO)
       .where(col(ent_id).isNotNull)
       .selectExpr(entId: _*)
       .write.parquet("/ori/treadMarkDF")


     //专利 18689672
     val patentDF = SparkHelper.getTableFromHbase(spark, SparkConstants.T_EDS_ENT_PATENT_INFO)
       .where(col(ent_id).isNotNull)
       .selectExpr(entId: _*)
       .write.parquet("/ori/patentDF")

     //营业执照是否有变更 102459172
     val alterationsDF = SparkHelper.getTableFromHbase(spark, SparkConstants.T_EDS_ENT_ALTERATIONS)
       .where(col(ent_id).isNotNull)
       .selectExpr(entId: _*)
       .write.parquet("/ori/alterationsDF")

     //是否有百度竞价推广 348333223
     val netPopuDF = SparkHelper.getTableFromHbase(spark, SparkConstants.T_EDS_ENT_NET_POPULARIZATION)
       .where(col(ent_id).isNotNull)
       .selectExpr(entId: _*)
       .write.parquet("/ori/netPopuDF")

     //是否有工公众号 1530941
     val subDF = SparkHelper.getTableFromHbase(spark, SparkConstants.T_EDS_ENT_SUBSCRIPTIONS)
       .where(col(ent_id).isNotNull)
       .selectExpr(entId: _*)
       .write.parquet("/ori/subDF")*/

    //商标 31697925
    val treadMarkDF = SparkHelper.getTableFromParquet(spark, "treadMarkDF")
      .where(col(ent_id).isNotNull)
      .selectExpr(entId: _*)
      .distinct()

    //专利 18689672
    val patentDF = SparkHelper.getTableFromParquet(spark, "patentDF")
      .where(col(ent_id).isNotNull)
      .selectExpr(entId: _*)
      .distinct()

    //营业执照是否有变更 102459172
    val alterationsDF = SparkHelper.getTableFromParquet(spark, "alterationsDF")
      .where(col(ent_id).isNotNull)
      .selectExpr(entId: _*)
      .distinct()

    //是否有百度竞价推广 348333223
    val netPopuDF = SparkHelper.getTableFromParquet(spark, "netPopuDF")
      .where(col(ent_id).isNotNull)
      .selectExpr(entId: _*)
      .distinct()

    //是否有工公众号 1530941
    val subDF = SparkHelper.getTableFromParquet(spark, "subDF")
      .where(col(ent_id).isNotNull)
      .selectExpr(entId: _*)
      .distinct()

    val sourceDF = spark.read.parquet("/ori/labelinfo1")
      .where(col(ent_id).isNotNull)
      .selectExpr(ent_id)
      .distinct()


    sourceDF.printSchema()
    treadMarkDF.printSchema()


    val labelDF = sourceDF
      .join(treadMarkDF, sourceDF(ent_id) === treadMarkDF(ent_id), SparkConstants.leftType)
      .join(patentDF, sourceDF(ent_id) === patentDF(ent_id), SparkConstants.leftType)
      .join(alterationsDF, sourceDF(ent_id) === alterationsDF(ent_id), SparkConstants.leftType)
      .join(netPopuDF, sourceDF(ent_id) === netPopuDF(ent_id), SparkConstants.leftType)
      .join(subDF, sourceDF(ent_id) === subDF(ent_id), SparkConstants.leftType)
      .select(
        sourceDF(ent_id),
        when(treadMarkDF(ent_id).isNotNull, 1).otherwise(2).alias("t1"),
        when(patentDF(ent_id).isNotNull, 1).otherwise(2).alias("t2"),
        when(alterationsDF(ent_id).isNotNull, 1).otherwise(2).alias("t3"),
        when(netPopuDF(ent_id).isNotNull, 1).otherwise(2).alias("t4"),
        when(subDF(ent_id).isNotNull, 1).otherwise(2).alias("t5")
      )

    println(labelDF.count())

    spark.read.parquet("/ori/result").repartition(1).write.parquet("")

  }
}
