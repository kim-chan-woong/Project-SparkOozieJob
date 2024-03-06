package org.Model

import org.apache.spark.sql._

object SparkModel {

  def createSession(appName: String): SparkSession = {

    SparkSession
      .builder
      .appName(appName)
      .config("spark.sql.warehouse.dir", ConfigModel.HIVE_WAREHOUSE_LOCATION)
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("partitionoverwritemode", "dynamic")
      .enableHiveSupport()
      .getOrCreate()
  }

}
