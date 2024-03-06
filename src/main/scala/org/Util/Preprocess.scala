package org.Util

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{File, FileOutputStream}
import java.util.zip.{ZipEntry, ZipOutputStream}

import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry
import java.io.{ByteArrayOutputStream, ByteArrayInputStream}

import org.Model.{ConfigModel, SparkModel}
import org.apache.commons.net.ftp.{FTP, FTPClient}

class Preprocess {


  // 분석 신호 json 메시지를 파싱하는 함수
  def kafka_value_parsing(kafka_value:String): java.util.Map[String,String] = {

    val objectMapper = new ObjectMapper()
    // ObjectMapper ' 인식 불가 -> " 변경
    val replace_value = kafka_value.replace("'", """"""")
    objectMapper.readValue(replace_value, classOf[java.util.Map[String, String]])

  }


  // ftp 내 이미지 저장 경로를 추출하는 함수
  def hive_select_img_location(spark: SparkSession, id:String, no:String, start_time:String, end_time:String): Array[Row] = {
    val hive_schema = "lake"
    val hive_table = "img_meta"

    // startTime, endTime이 같을 시의 쿼리
    if (start_time.substring(0,6) == end_time.substring(0,6)) {

      val upload_dttm = start_time.substring(0,6)

      // 테스트용 쿼리
//      spark.sql(
//        f"""
//           |select img_nm, img_path
//           |from $hive_schema%s.$hive_table%s
//           |where upload_dttm = '202310'
//           |and id = 'TG114'
//           |limit 10
//           |""".stripMargin).select(concat(col("img_path"), col("img_nm")).alias("path")).collect()

      spark.sql(
        f"""
           |select img_nm, img_path
           |from $hive_schema%s.$hive_table%s
           |where upload_dttm = '$upload_dttm%s'
           |and id = '$id%s'
           |and no = '$no%s'
           |and upload_dttm >= '$start_time%s'
           |and upload_dttm <= '$end_time%s'
           |""".stripMargin).select(concat(col("img_path"), col("img_nm")).alias("path")).collect()

      // startTime, endTime이 다를 시의 쿼리
    } else {

      val upload_dttm_start = start_time.substring(0,6)
      val upload_dttm_end = end_time.substring(0,6)

      spark.sql(
        f"""
           |select img_nm, img_path
           |from $hive_schema%s.$hive_table%s
           |where (upload_dttm = '$upload_dttm_start%s' or upload_dttm = '$upload_dttm_end%s')
           |and id = '$id%s'
           |and no = '$no%s'
           |and upload_dttm >= '$start_time%s'
           |and upload_dttm <= '$end_time%s'
           |""".stripMargin).select(concat(col("img_path"), col("img_nm")).alias("path")).collect()
    }

  }

  // hive 테이블 select 후 csv 추출 (return String)
  def hive_select_csv_data(spark: SparkSession, id:String, no:String, start_time:String, end_time:String): String = {
    val hive_schema = "lake"
    val hive_table = "analysis_result"

    // startTime, endTime이 같을 시의 쿼리
    if (start_time.substring(0,6) == end_time.substring(0,6)) {

      val work_time = start_time.substring(0,6)

      // 테스트용 쿼리
//      val df = spark.sql(
//        f"""
//           |select *
//           |from $hive_schema%s.$hive_table%s
//           |where work_time = '202310'
//           |and id = 'TG125'
//           |and no = '3H556'
//           |limit 10
//           |""".stripMargin)
//
//      df.columns.mkString(",") + "\n" + df.collect().map(_.mkString(",")).mkString("\n")

      val df = spark.sql(
        f"""
           |select *
           |from $hive_schema%s.$hive_table%s
           |where work_time = '$work_time%s'
           |and id = '$id%s'
           |and no = '$no%s'
           |and upload_dttm >= '$start_time%s'
           |and upload_dttm <= '$end_time%s'
           |""".stripMargin)

      // 헤더 추가
      df.columns.mkString(",") + "\n" + df.collect().map(_.mkString(",")).mkString("\n")

      // startTime, endTime이 다를 시의 쿼리
    } else {

      val work_time_start = start_time.substring(0,6)
      val work_time_end = end_time.substring(0,6)

      val df = spark.sql(
        f"""
           |select *
           |from $hive_schema%s.$hive_table%s
           |where (work_time = '$work_time_start%s' or work_time = '$work_time_end%s')
           |and id = '$id%s'
           |and no = '$no%s'
           |and upload_dttm >= '$start_time%s'
           |and upload_dttm <= '$end_time%s'
           |""".stripMargin)

      // 헤더 추가
      df.columns.mkString(",") + "\n" + df.collect().map(_.mkString(",")).mkString("\n")

    }
  }

  // zip파일 생성 함수
  // 1. ftp 세션연결
  // 2. image_list 내 경로로 ftp서버의 이미지들을 가져옴
  // 3. outputStream에 바이트 배열로 input
  // 4. 이미지 파일들 모두 가져온 후, csv(String) outputStream에 input
  def create_zip(image_list:Array[Row], csv_data: String, id:String, no: String): Array[Byte] = {

    // ftp 연결
    val ftpClient = new FTPClient()
    ftpClient.connect(ConfigModel.FTP_HOST)
    ftpClient.login(ConfigModel.FTP_USER, ConfigModel.FTP_PASSWORD)
    ftpClient.enterLocalPassiveMode()
    ftpClient.setFileType(FTP.BINARY_FILE_TYPE)

    try {
      // zip파일에 담길 데이터 스트림
      val outputStream = new ByteArrayOutputStream()
      val zipOutputStream = new ZipArchiveOutputStream(outputStream)

      // ftp서버의 파일을 읽어 outputStream에 저장
      image_list.foreach { row =>
        val img_full_path = row(0).toString
        val img_name = img_full_path.split('/').last

        // 압축 파일 내 각 파일들의 이름
        val entry = new ZipArchiveEntry(img_name)
        zipOutputStream.putArchiveEntry(entry)

        val fileStream = ftpClient.retrieveFileStream(img_full_path)
        val buffer = new Array[Byte](1024)
        var bytesRead = fileStream.read(buffer)

        // 파일의 모든 내용을 읽을 때 까지  반복
        while (bytesRead != -1) {
          zipOutputStream.write(buffer, 0, bytesRead)
          bytesRead = fileStream.read(buffer)
        }
        // 이 구문 넣어줘야 연속해서 파일들 읽을 때 널포인터 에러 안남
        if (fileStream!=null) {
          fileStream.close()
        }
        ftpClient.completePendingCommand()
        zipOutputStream.closeArchiveEntry()

      }

      // 이미지 파일들 저장 후 csv파일 저장
      val csv_entry = new ZipArchiveEntry(f"$id%s_$no%s.csv")
      zipOutputStream.putArchiveEntry(csv_entry)

      zipOutputStream.write(csv_data.getBytes("UTF-8"))
      zipOutputStream.closeArchiveEntry()

      zipOutputStream.finish()
      zipOutputStream.close()

      outputStream.toByteArray

    } finally {
      ftpClient.logout()
      ftpClient.disconnect()
    }


  }

  // zip파일 hdfs 업로드 함수
  def upload_hdfs(spark:SparkSession, zip_bytes:Array[Byte], id:String, no: String): Unit = {
    val default_path = ConfigModel.UPLOAD_HDFS_PATH
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    // hdfs 경로 생성
    if (!fs.exists(new Path(f"$default_path%s/$id%s"))) {
      fs.mkdirs(new Path(f"$default_path%s/$id%s"))
    }

    if (!fs.exists(new Path(f"$default_path%s/$id%s/$no%s"))) {
      fs.mkdirs(new Path(f"$default_path%s/$id%s/$no%s"))
    }

    // zip 파일이름
    val upload_name = f"$id%s_$no%s.zip"
    val upload_path = f"$default_path%s/$id%s/$no%s/"
    val hdfs_output_stream = fs.create(new Path(upload_path + upload_name))

    hdfs_output_stream.write(zip_bytes)
    hdfs_output_stream.close()
  }


}
