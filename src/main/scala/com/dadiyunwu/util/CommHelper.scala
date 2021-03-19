package com.dadiyunwu.util

import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.io.Source

object CommHelper {

  def getFilePath(cls: Class[_], fileName: String): String = {

    val path = cls.getClassLoader.getResource(fileName).getPath
    path
  }

  def readFile2Map4String(cls: Class[_], file: String): mutable.HashMap[String, String] = {
    val path = CommHelper.getFilePath(cls, file)
    val lines = Source.fromFile(path).getLines()

    val map = new mutable.HashMap[String, String]()
    lines.foreach(elem => {
      val arr = elem.split(",")
      map.put(arr(0), arr(1))
    })
    map
  }

  def readFile2Map4StringFromSpark(spark: SparkSession, fileName: String): mutable.HashMap[String, String] = {
    val map = new mutable.HashMap[String, String]()

    val path = SparkHelper.getFilePath(fileName)
    val strs: Array[String] = spark.sparkContext.textFile(path).collect()
    strs.foreach(elem => {
      val arr = elem.split(",")
      map.put(arr(0), arr(1))
    })
    map
  }

}
