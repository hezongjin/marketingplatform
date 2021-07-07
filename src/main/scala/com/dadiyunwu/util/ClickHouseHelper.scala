package com.dadiyunwu.util

import java.sql.Statement

import com.dadiyunwu.comm.SparkConstants
import ru.yandex.clickhouse.{ClickHouseConnection, ClickHouseDataSource}
import ru.yandex.clickhouse.settings.ClickHouseProperties

object ClickHouseHelper {

  def getCKConnection(): ClickHouseConnection = {

    val properties = new ClickHouseProperties()
    properties.setUser(SparkConstants.CLICKHOUSE_USER);
    properties.setPassword(SparkConstants.CLICKHOUSE_PASSWORD)
    properties.setConnectionTimeout(60 * 1000);

    val url = SparkConstants.CLICKHOUSE_URL()
    val clickHouseDataSource = new ClickHouseDataSource(url, properties)

    val connection = clickHouseDataSource.getConnection

    connection
  }

  def executeSql(conn: ClickHouseConnection, sql: String): Boolean = {
    var bool: Boolean = false
    var statement: Statement = null
    try {
      statement = conn.createStatement()
      bool = statement.execute(sql)
    } catch {
      case ex: Exception => ex.printStackTrace()
    } finally {
      try {
        if (null != statement) {
          statement.close()
        }
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
    bool
  }


  def closeCKConnection(conn: ClickHouseConnection): Unit = {

    try {
      if (null != conn) {
        conn.close()
      }
    } catch {
      case e: Exception => e.getMessage()
    }

  }

}
