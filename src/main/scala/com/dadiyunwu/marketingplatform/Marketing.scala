package com.dadiyunwu.marketingplatform

import com.dadiyunwu.comm.SparkConstants
import com.dadiyunwu.util.{ColumnHelper, CommHelper, SparkHelper}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, RowFactory}
import org.apache.spark.sql.catalyst.encoders.RowEncoder

import scala.collection.mutable.ArrayBuffer

object Marketing {

  private val ent_id = "ent_id"

  def main(args: Array[String]): Unit = {

    val conf = SparkHelper.getSparkConf(SparkConstants.name)
    //      .set("spark.sql.warehouse.dir", "spark-warehouse")
    val spark = SparkHelper.getSparkSession(conf)

    val industryMap = CommHelper.readFile2Map4String("industry.txt")
    val regionMap = CommHelper.readFile2Map4String("region.txt")

    val sourceDF = spark.read.parquet("/ori/labelinfo1")
      .where(col(ent_id).isNotNull)
      //      .selectExpr("ent_id", "cust_id", "createDate", "custNameCn")
      .selectExpr(ent_id)
      .repartition(100)

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
        sourceDF(ent_id),
        when(treadMarkDF(ent_id).isNotNull, 1).otherwise(2) alias ("t1"),
        when(patentDF(ent_id).isNotNull, 1).otherwise(2).alias("t2"),
        when(alterationsDF(ent_id).isNotNull, 1).otherwise(2).alias("t3"),
        when(netPopuDF(ent_id).isNotNull, 1).otherwise(2).alias("t4"),
        when(subDF(ent_id).isNotNull, 1).otherwise(2).alias("t5")
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
    val resultDF = labelWithCapitalDF.join(baseInfoDF, Seq(ent_id), SparkConstants.leftType)
      .select(baseInfoJoinColumns: _*)

    val resultSchema = ColumnHelper.getResultSchema()
    val resultEncoder = RowEncoder(resultSchema)

    val resultData = resultDF.mapPartitions(iter => {
      val arr = new ArrayBuffer[Row]()
      iter.foreach(row => {
        val ent_id: String = row.getAs[String]("ent_id")
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

        arr += RowFactory.create(ent_id, t1, t2, t3, t4, t5, t6, reg_caps2,
          estimate_date, cnei, cnei_sec, province, city, county
        )
      })
      arr.iterator
    })(resultEncoder)

    resultData.show()

  }
}
