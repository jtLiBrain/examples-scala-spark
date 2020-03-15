package org.apache.spark.sql.hive.jtLiBrain

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveExternalCatalog

/**
  * 这是一个非常有帮助的类，可以使用它获取hive表的元数据信息、创建表等
  * @param conf
  * @param hadoopConf
  */
class HiveExternalCatalogBridge(conf: SparkConf, hadoopConf: Configuration) extends HiveExternalCatalog(conf, hadoopConf) {
}
