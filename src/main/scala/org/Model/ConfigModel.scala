package org.Model

object ConfigModel {
  private val _APP_NAME = "SPARK_OOZIE_JOB_01_TEST"
  private val _LOCAL_PATH = ""
//  private val _REMOTE_PATH = "hdfs://hadoopHA:8020/user/spark/test/result_upload"
// °ª ¼¼ÆÃ
  private val _FTP_HOST = ""
  private val _FTP_PASSWORD = ""
  private val _FTP_USER = ""
  private val _HIVE_WAREHOUSE_LOCATION = "hdfs://hadoopHA:8020/hive/lake/"
  private val _UPLOAD_HDFS_PATH = "hdfs:///user/spark/test/anlaysis_result_upload"


  def APP_NAME: String = this._APP_NAME

  def LOCAL_PATH: String = this._LOCAL_PATH

//  def REMOTE_PATH: String = this._REMOTE_PATH

  def FTP_HOST: String = this._FTP_HOST

  def FTP_PASSWORD: String = this._FTP_PASSWORD

  def FTP_USER: String = this._FTP_USER

  def HIVE_WAREHOUSE_LOCATION: String = this._HIVE_WAREHOUSE_LOCATION

  def UPLOAD_HDFS_PATH: String = this._UPLOAD_HDFS_PATH


}
