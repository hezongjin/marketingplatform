package com.dadiyunwu.comm

import java.util.Properties

import scala.util.Random

object SparkConstants {

  val tag = "prod"

  //================================spark参数=====================================
  //  val master = "yarn"
  val master = if (tag.equals("test")) {
    "local[*]"
  } else {
    "yarn"
  }
  val name = "Marketing"
  val leftType = "left"


  //================================phoenix连接参数===============================
  val phoenix = "org.apache.phoenix.spark"
  val zkUrl = "zkUrl"
  val table = "table"
  val zkSer = "server1,server2,tool:2181:/hbase-unsecure"

  //================================表名==========================================
  val T_EDS_ENT_BASE_INFO = "EDS.T_EDS_ENT_BASE_INFO"
  val T_EDS_ENT_BASE_INFO_CAPITAL = "EDS_BAK.T_EDS_ENT_BASE_INFO_CAPITAL"
  val T_EDS_ENT_TRADEMARK_INFO = "EDS.T_EDS_ENT_TRADEMARK_INFO"
  val T_EDS_ENT_PATENT_INFO = "EDS.T_EDS_ENT_PATENT_INFO"
  val T_EDS_ENT_ALTERATIONS = "EDS.T_EDS_ENT_ALTERATIONS"
  val T_EDS_ENT_NET_POPULARIZATION = "EDS.T_EDS_ENT_NET_POPULARIZATION"
  val T_EDS_ENT_SUBSCRIPTIONS = "EDS.T_EDS_ENT_SUBSCRIPTIONS"

  //================================Map==========================================

  /**
    * "createDate" -> "建档时间",
    * "custNameCn" -> "客户名称",
    * "t1" -> "是否有商标(1有 2无)",
    * "t2" -> "是否有专利(1有 2无)",
    * "t3" -> "营业执照是否有变更(1是 2否)",
    * "t4" -> "是否有百度竞价推广(1有 2无)",
    * "t5" -> "是否有公众号(1有 2无)",
    * "t6" -> "是否含进出口(1有 2无)",
    * "reg_caps2" -> "注册资金(万人民币)",
    * "estimate_date" -> "成立日期",
    * "cnei" -> "所属行业-一级",
    * "cnei_sec" -> "所属行业-二级",
    * "province" -> "注册地址-省",
    * "city" -> "注册地址-市",
    * "county" -> "注册地址-区"
    */
  val TAG_VALUE_NAME_MAP1 = Map(
    "t1" -> "是否有商标(1有 2无)",
    "t2" -> "是否有专利(1有 2无)",
    //    "t3" -> "营业执照是否有变更(1是 2否)",
    "t4" -> "是否有百度竞价推广(1有 2无)",
    "t5" -> "是否有公众号(1有 2无)",
    "t6" -> "是否含进出口(1有 2无)"
  )

  val TAG_VALUE_NAME_MAP2 = Map(
    "t3" -> "营业执照是否有变更(1是 2否)"
  )

  val TAG_VALUE_NAME_MAP3 = Map(
    "cnei_code" -> "所属行业-一级",
    "cnei_sec_code" -> "所属行业-二级"
  )

  val TAG_VALUE_NAME_MAP4 = Map(
    "province_code" -> "注册地址-省",
    "city_code" -> "注册地址-市",
    "county_code" -> "注册地址-区"
  )

  val TAG_VALUE_NAME_MAP5 = Map(
    "createDate" -> "建档时间",
    "custNameCn" -> "客户名称",
    "reg_caps2" -> "注册资金(万人民币)",
    "estimate_date" -> "成立日期"
  )


  val TAG_CODE_MAP = Map(
    "createDate" -> "CRE_DATE",
    "custNameCn" -> "CUST_NAME",
    "t1" -> "IS_TRADEMARK",
    "t2" -> "IS_PATENT",
    "t3" -> "IS_CHANGE_LICENSE",
    "t4" -> "IS_PROMOTION",
    "t5" -> "IS_OFFICIAL_ACCOUNT",
    "t6" -> "IS_IO",
    "reg_caps2" -> "REG_CAPS",
    "estimate_date" -> "ESTIMATE_DATE",
    "cnei_code" -> "CNEI_CODE",
    "cnei_sec_code" -> "CNEI_SEC_CODE",
    "province_code" -> "PROVINCE",
    "city_code" -> "CITY",
    "county_code" -> "COUNTY"
  )

  //======================================clickhouse===============================================

  //生产环境
  val CLICKHOUSE_URLS = if (tag.equals("prod")) {
    """
      |jdbc:clickhouse://10.112.2.9:8123/ODS_LOCAL?useUnicode=true&characterEncoding=UTF-8,
      |jdbc:clickhouse://10.112.2.10:8123/ODS_LOCAL?useUnicode=true&characterEncoding=UTF-8,
      |jdbc:clickhouse://10.112.2.26:8123/ODS_LOCAL?useUnicode=true&characterEncoding=UTF-8,
      |jdbc:clickhouse://10.112.2.35:8123/ODS_LOCAL?useUnicode=true&characterEncoding=UTF-8
    """.stripMargin
  } else if (tag.equals("test")) {
    """
      |jdbc:clickhouse://10.112.1.15:8123/ODS_LOCAL?useUnicode=true&characterEncoding=UTF-8,
      |jdbc:clickhouse://10.112.1.24:8123/ODS_LOCAL?useUnicode=true&characterEncoding=UTF-8,
      |jdbc:clickhouse://10.112.1.43:8123/ODS_LOCAL?useUnicode=true&characterEncoding=UTF-8
    """.stripMargin
  } else {
    null
  }

  val CLICKHOUSE_DERIVER = classOf[ru.yandex.clickhouse.ClickHouseDriver].getName
  val CLICKHOUSE_USER = "default"
  val CLICKHOUSE_PASSWORD = if (tag.equals("prod")) {
    "FqB4Ml6T"
  } else if (tag.equals("test")) {
    "c6hP16Fd"
  } else {
    null
  }

  val CLICKHOUSE_PROP = {
    val prop = new Properties()
    prop.put("driver", SparkConstants.CLICKHOUSE_DERIVER)
    prop.put("user", SparkConstants.CLICKHOUSE_USER)
    prop.put("password", SparkConstants.CLICKHOUSE_PASSWORD)
    prop
  }

  val CLICKHOUSE_URL = () => {
    val arr = CLICKHOUSE_URLS.split(",")
    val url = arr(Random.nextInt(arr.length)).trim
    url
  }

  val DATA_TABLE = "DW.DW_CE17_TAG_CUST_BASE"

  //======================================file=====================================================


  def main(args: Array[String]): Unit = {

    println(CLICKHOUSE_PROP)

  }

}


