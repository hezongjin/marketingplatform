package com.dadiyunwu.comm

object SparkConstants {

  //================================spark参数=====================================
  //  val master = "yarn"
  val master = "local[2]"
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


}
