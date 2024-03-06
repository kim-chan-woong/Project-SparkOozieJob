ThisBuild / version := "0.1.0-SNAPSHOT-TEST"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "SPARK_OOZIE_JOB"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.3",
  "org.apache.spark" %% "spark-sql" % "3.2.3",
  "org.apache.hadoop" % "hadoop-common" % "3.2.1",
  "commons-net" % "commons-net" % "3.8.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.12.5"
  //  "org.apache.oozie" % "oozie-client" % "5.2.1"
)

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  "SPARK_OOZIE_JOB-0.1.0-SNAPSHOT-TEST.jar"
}

