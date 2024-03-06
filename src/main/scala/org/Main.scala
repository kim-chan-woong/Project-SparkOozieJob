package org

import org.Model.{ConfigModel, SparkModel}
import org.Util.Preprocess
import org.apache.commons.net.ftp.{FTP, FTPClient}


object Main {

  def main(args: Array[String]): Unit = {

    val preprocess = new Preprocess

    // spark 세션을 생성합니다.
    val spark = SparkModel.createSession(ConfigModel.APP_NAME)


    try {

      // kafka 메시지를 파싱합니다.
      val kafka_map = preprocess.kafka_value_parsing(args(0))

      // 각 키 값을 추출합니다.
      val id = kafka_map.get("Id")
      val no = kafka_map.get("No")
      val start_time = kafka_map.get("startTime")
      val end_time = kafka_map.get("endTime")

      // 이미지가 저장된 ftp 경로를 추출합니다.
      val image_list = preprocess.hive_select_img_location(spark, id, no, start_time, end_time)

      // hive 테이블 조회 후 문자열로 리턴받습니다.
      val csv_data = preprocess.hive_select_csv_data(spark, id, no, start_time, end_time)

      // 이미지들과 csv를 압축하여 zip파일로 변환합니다.
      val zip_bytes = preprocess.create_zip(image_list, csv_data, id, no)

      // zip파일을 HDFS에 업로드합니다.
      preprocess.upload_hdfs(spark, zip_bytes, id, no)


    } catch {
      case ex: Exception => println(ex)
    } finally {
      // spark 세션을 종료합니다.
      spark.stop()
    }

  }
}
