package com.bigdata.hisign.sparkstreaming2

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionSingleton extends Serializable {
  @transient private var instance: SparkSession = _
  def getInstance(sparkConfig:SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConfig)
        .enableHiveSupport()
        .getOrCreate()
    }
    instance
  }
}
