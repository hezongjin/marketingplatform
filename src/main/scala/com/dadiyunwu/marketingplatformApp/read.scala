package com.dadiyunwu.marketingplatformApp

import com.dadiyunwu.util.{ColumnHelper, CommHelper, SparkHelper}
import org.apache.spark.sql.{Row, RowFactory}
import org.apache.spark.sql.catalyst.encoders.RowEncoder

import scala.collection.mutable.ArrayBuffer


object read {

  val url = "part-00000-67302b3b-fca0-482c-890a-afc9d7120bc1-c000.snappy.parquet"

  def main(args: Array[String]): Unit = {

    val conf = SparkHelper.getSparkConf("read")
    val spark = SparkHelper.getSparkSession(conf)

    val path = CommHelper.getFilePath(read.getClass, url)
    val df = spark.read.parquet(path)

    val industryMap = CommHelper.readFile2Map4String(read.getClass, "industry.txt")
    val regionMap = CommHelper.readFile2Map4String(read.getClass, "region.txt")


    val resultSchema = ColumnHelper.getResultSchema()
    val resultEncoder = RowEncoder(resultSchema)

    val resultDF = df.mapPartitions(iter => {
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

    resultDF
      .filter("instr(cnei,'制造业') >= 1")
      .show()

  }
}
