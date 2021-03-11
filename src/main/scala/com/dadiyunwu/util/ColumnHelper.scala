package com.dadiyunwu.util

import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ColumnHelper {

  def getEntID(): ArrayBuffer[String] = {
    val columns = new ArrayBuffer[String]()
    columns.+=("ent_id")
    columns
  }

  def getBaseInfoJoinColumns(labelWithCapitalDF: DataFrame): ArrayBuffer[Column] = {
    val columns = new ArrayBuffer[Column]()
    columns.+=(labelWithCapitalDF("*"))
    columns.+=(col("estimate_date"))
    columns.+=(col("cnei_code"))
    columns.+=(col("cnei_sec_code"))
    columns.+=(col("province_code"))
    columns.+=(col("city_code"))
    columns.+=(col("county_code"))
    columns.+=(
      when(
        instr(col("oper_scope"), "外贸") >= 0
          or instr(col("oper_scope"), "国际贸易") >= 0
          or instr(col("oper_scope"), "进出口") >= 0
          or instr(col("oper_scope"), "对外贸易") >= 0
          or instr(col("oper_scope"), "货运代理") >= 0
          or instr(col("oper_scope"), "国际货运") >= 0
        , 1).otherwise(2).alias("t6"))
    columns
  }

  def getBaseInfoColumns(): ArrayBuffer[String] = {
    val columns = new ArrayBuffer[String]()
    columns.+=("ent_id")
    columns.+=("estimate_date")
    columns.+=("cnei_code")
    columns.+=("cnei_sec_code")
    columns.+=("province_code")
    columns.+=("city_code")
    columns.+=("county_code")
    columns.+=("oper_scope")

    columns
  }

  def getResultSchema(): StructType = {
    val schea = StructType(Seq(
      StructField("ent_id", StringType),
      StructField("t1", IntegerType),
      StructField("t2", IntegerType),
      StructField("t3", IntegerType),
      StructField("t4", IntegerType),
      StructField("t5", IntegerType),
      StructField("t6", IntegerType),
      StructField("reg_caps2", StringType),
      StructField("estimate_date", StringType),
      StructField("cnei", StringType),
      StructField("cnei_sec", StringType),
      StructField("province", StringType),
      StructField("city", StringType),
      StructField("county", StringType)
    ))
    schea
  }

}
