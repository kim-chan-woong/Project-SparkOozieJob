# Spark Oozie Job
 
<br>
 Spark Streaming에서 실행하는 Ooize Job입니다.
 
HDFS Upload 기능입니다.
 
<br><br>
 

 
1. 이전 Spark Streaming에서 받은 Kafka 메시지를 파싱합니다.
 
2. 필요한 key, value들을 가져와 hive table(이미지 메타데이터)을 조회 후, ftp 서버 내 이미지가 저장된 경로를 가져옵니다. 
 
3. ftp서버에 연결하여 저장경로의 이미지 파일들을 가져옵니다.
 
4. 필요한 key, value들을 가져와 hive table(이미지 분석 결과)을 조회 후, 조회 결과를 csv화합니다.
 
5. 이미지 파일들과, csv 데이터를 zip파일로 압축 합니다.
 
6. zip파일을 HDFS상에 업로드 합니다.
 
7. 전달받은 kafka 메시지의 내용을 조합하여 HDFS상에 업로드됩니다.
