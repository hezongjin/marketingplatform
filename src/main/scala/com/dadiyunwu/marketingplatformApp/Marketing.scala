package com.dadiyunwu.marketingplatformApp

import java.time.LocalDate

import com.dadiyunwu.comm.SparkConstants
import com.dadiyunwu.util.{ColumnHelper, CommHelper, SparkHelper}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, RowFactory, SaveMode}
import org.apache.spark.sql.catalyst.encoders.RowEncoder

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 标签计算 写入CK
  */
object Marketing {

  private val ent_id = "ent_id"

  def main(args: Array[String]): Unit = {

    val conf = SparkHelper.getSparkConf(SparkConstants.name)
    //      .set("spark.sql.warehouse.dir", "spark-warehouse")
    val spark = SparkHelper.getSparkSession(conf)

    val sourceDF = spark.read.parquet("/ori/marketing")
      .where(col("entId").isNotNull)
      .selectExpr("entId as ent_id", "custId as cust_id", "createDate", "custNameCn")
      .repartition(20)

    val entId = ColumnHelper.getEntID()

    //商标 31697925
    val treadMarkDF = SparkHelper.getTableFromHbase(spark, SparkConstants.T_EDS_ENT_TRADEMARK_INFO)
      .where(col(ent_id).isNotNull)
      .selectExpr(entId: _*)
      .distinct()

    //专利 18689672
    val patentDF = SparkHelper.getTableFromHbase(spark, SparkConstants.T_EDS_ENT_PATENT_INFO)
      .where(col(ent_id).isNotNull)
      .selectExpr(entId: _*)
      .distinct()

    //营业执照是否有变更 102459172
    val alterationsDF = SparkHelper.getTableFromHbase(spark, SparkConstants.T_EDS_ENT_ALTERATIONS)
      .where(col(ent_id).isNotNull)
      .selectExpr(entId: _*)
      .distinct()

    //是否有百度竞价推广 348333223
    val netPopuDF = SparkHelper.getTableFromHbase(spark, SparkConstants.T_EDS_ENT_NET_POPULARIZATION)
      .where(col(ent_id).isNotNull)
      .selectExpr(entId: _*)
      .distinct()

    //是否有工公众号 1530941
    val subDF = SparkHelper.getTableFromHbase(spark, SparkConstants.T_EDS_ENT_SUBSCRIPTIONS)
      .where(col(ent_id).isNotNull)
      .selectExpr(entId: _*)
      .distinct()


    val labelDF = sourceDF
      .join(treadMarkDF, sourceDF(ent_id) === treadMarkDF(ent_id), SparkConstants.leftType)
      .join(patentDF, sourceDF(ent_id) === patentDF(ent_id), SparkConstants.leftType)
      .join(alterationsDF, sourceDF(ent_id) === alterationsDF(ent_id), SparkConstants.leftType)
      .join(netPopuDF, sourceDF(ent_id) === netPopuDF(ent_id), SparkConstants.leftType)
      .join(subDF, sourceDF(ent_id) === subDF(ent_id), SparkConstants.leftType)
      .select(
        sourceDF("*"),
        when(treadMarkDF(ent_id).isNotNull, 1).otherwise(0).alias("t1"),
        when(patentDF(ent_id).isNotNull, 1).otherwise(0).alias("t2"),
        when(alterationsDF(ent_id).isNotNull, 1).otherwise(0).alias("t3"),
        when(netPopuDF(ent_id).isNotNull, 1).otherwise(0).alias("t4"),
        when(subDF(ent_id).isNotNull, 1).otherwise(0).alias("t5")
      )


    val capitalDF = SparkHelper.getTableFromHbase(spark, SparkConstants.T_EDS_ENT_BASE_INFO_CAPITAL)
      .selectExpr("ent_id", "reg_caps2")
      .distinct()


    val labelWithCapitalDF = labelDF.join(capitalDF, Seq(ent_id), SparkConstants.leftType)
      .select(labelDF("*"), capitalDF("reg_caps2").alias("reg_caps2"))

    val baseInfoColumns = ColumnHelper.getBaseInfoColumns()
    val baseInfoDF = SparkHelper.getTableFromHbase(spark, SparkConstants.T_EDS_ENT_BASE_INFO)
      .selectExpr(baseInfoColumns: _*)


    val baseInfoJoinColumns = ColumnHelper.getBaseInfoJoinColumns(labelWithCapitalDF)
    val oldResultDF = labelWithCapitalDF.join(baseInfoDF, Seq(ent_id), SparkConstants.leftType)
      .select(baseInfoJoinColumns: _*)

    val oriDF = spark.read.parquet("/ori/marketing")

    val resultDF = oriDF.join(oldResultDF, oriDF("custId") === oldResultDF("cust_id"), "left")
      .select(
        oriDF("custId").alias("cust_id"),
        oriDF("createDate"),
        oriDF("custNameCn"),
        col("t1"),
        col("t2"),
        col("t3"),
        col("t4"),
        col("t5"),
        col("t6"),
        col("reg_caps2"),
        col("estimate_date"),
        col("cnei_code"),
        col("cnei_sec_code"),
        col("province_code"),
        col("city_code"),
        col("county_code")
      )

    //.coalesce(20)                 //OOM


    /*val resultSchema = ColumnHelper.getResultSchema()
    val resultEncoder = RowEncoder(resultSchema)*/

    /*val resultData = resultDF.mapPartitions(iter => {
      val arr = new ArrayBuffer[Row]()
      iter.foreach(row => {
        val ent_id: String = row.getAs[String]("ent_id")
        val cust_id: String = row.getAs[String]("cust_id")
        val createDate: String = row.getAs[String]("createDate")
        val custNameCn: String = row.getAs[String]("custNameCn")
        val t1: AnyRef = row.getAs[Nothing]("t1")
        val t2: AnyRef = row.getAs[Nothing]("t2")
        val t3: AnyRef = row.getAs[Nothing]("t3")
        val t4: AnyRef = row.getAs[Nothing]("t4")
        val t5: AnyRef = row.getAs[Nothing]("t5")
        val t6: AnyRef = row.getAs[Nothing]("t6")
        val reg_caps2: String = row.getAs[Nothing]("reg_caps2")
        val estimate_date: String = row.getAs[Nothing]("estimate_date")
        val cnei_code: String = row.getAs[Nothing]("cnei_code")
        val cnei_sec_code: String = row.getAs[Nothing]("cnei_sec_code")
        val province_code: String = row.getAs[Nothing]("province_code")
        val city_code: String = row.getAs[Nothing]("city_code")
        val county_code: String = row.getAs[Nothing]("county_code")

        val cnei = industryMap.getOrElse(cnei_code, "")
        val cnei_sec = industryMap.getOrElse(cnei_sec_code, "")
        val province = regionMap.getOrElse(province_code, "")
        val city = regionMap.getOrElse(city_code, "")
        val county = regionMap.getOrElse(county_code, "")

        arr += RowFactory.create(ent_id, cust_id, createDate, custNameCn, t1, t2, t3, t4, t5, t6, reg_caps2,
          estimate_date, cnei, cnei_sec, province, city, county
        )
      })
      arr.iterator
    })(resultEncoder)*/

    resultDF.persist()

    val today = LocalDate.now().plusDays(-1).toString
    val prop = SparkConstants.CLICKHOUSE_PROP
    val url = SparkConstants.CLICKHOUSE_URL()
    val table = SparkConstants.DATA_TABLE

    for (elem <- SparkConstants.TAG_VALUE_NAME_MAP1) {
      val key = elem._1
      val value = elem._2
      resultDF
        .select(col("cust_id").alias("CUST_ID"), col(key).alias("TAG_VALUE"),
          when(col(key).equalTo(1), "有").when(col(key).equalTo("0"), "无").otherwise(null).alias("TAG_VALUE_NAME")
        )
        .withColumn("DATA_DATE", lit(today))
        //        .withColumn("TAG_VALUE_NAME", lit(SparkConstants.TAG_VALUE_NAME_MAP1(key)))
        .withColumn("TAG_CODE", lit(SparkConstants.TAG_CODE_MAP(key)))
        .write
        .mode(SaveMode.Append)
        .jdbc(url, table, prop)

      println("=" * 32)
    }

    for (elem <- SparkConstants.TAG_VALUE_NAME_MAP2) {
      val key = elem._1
      val value = elem._2
      resultDF
        .select(col("cust_id").alias("CUST_ID"), col(key).alias("TAG_VALUE"),
          when(col(key).equalTo(1), "是").when(col(key).equalTo(0), "否").otherwise(null).alias("TAG_VALUE_NAME")
        )
        .withColumn("DATA_DATE", lit(today))
        //        .withColumn("TAG_VALUE_NAME", lit(SparkConstants.TAG_VALUE_NAME_MAP1(key)))
        .withColumn("TAG_CODE", lit(SparkConstants.TAG_CODE_MAP(key)))
        .write
        .mode(SaveMode.Append)
        .jdbc(url, table, prop)

      println("=" * 32)
    }

    for (elem <- SparkConstants.TAG_VALUE_NAME_MAP3) {
      val key = elem._1
      val value = elem._2
      resultDF
        //        .select(col("cust_id").alias("CUST_ID"), col(key).alias("TAG_VALUE"))
        .selectExpr("cust_id as CUST_ID", s"${key} as TAG_VALUE", s"getIndus(${key}) as TAG_VALUE_NAME")
        .withColumn("DATA_DATE", lit(today))
        .withColumn("TAG_CODE", lit(SparkConstants.TAG_CODE_MAP(key)))
        .write
        .mode(SaveMode.Append)
        .jdbc(url, table, prop)
    }

    for (elem <- SparkConstants.TAG_VALUE_NAME_MAP4) {
      val key = elem._1
      val value = elem._2
      resultDF
        //        .select(col("cust_id").alias("CUST_ID"), col(key).alias("TAG_VALUE"))
        .selectExpr("cust_id as CUST_ID", s"${key} as TAG_VALUE", s"getRegion(${key}) as TAG_VALUE_NAME")
        .withColumn("DATA_DATE", lit(today))
        .withColumn("TAG_CODE", lit(SparkConstants.TAG_CODE_MAP(key)))
        .write
        .mode(SaveMode.Append)
        .jdbc(url, table, prop)

    }

    for (elem <- SparkConstants.TAG_VALUE_NAME_MAP5) {
      val key = elem._1
      val value = elem._2
      resultDF
        .select(col("cust_id").alias("CUST_ID"), col(key).alias("TAG_VALUE"))
        .withColumn("DATA_DATE", lit(today))
        .withColumn("TAG_CODE", lit(SparkConstants.TAG_CODE_MAP(key)))
        .write
        .mode(SaveMode.Append)
        .jdbc(url, table, prop)

      println("=" * 32)
    }


    resultDF.unpersist()

    spark.sparkContext.stop()
    spark.stop()
  }

}
