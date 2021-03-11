package com.dadiyunwu.util

import scala.collection.mutable
import scala.io.Source

object CommHelper {

  def getFilePath(fileName: String): String = {

    val path = CommHelper.getClass.getClassLoader.getResource(fileName).getPath
    path
  }

  def readFile2Map4String(file: String): mutable.HashMap[String, String] = {
    val path = CommHelper.getFilePath(file)
    val lines = Source.fromFile(path).getLines()

    val map = new mutable.HashMap[String, String]()
    lines.foreach(elem => {
      val arr = elem.split(",")
      map.put(arr(0), arr(1))
    })
    map
  }

}
