import sbt._

object Dependencies {
  // versions
  lazy val sparkVersion = "3.3.2"
  lazy val hadoopVersion = "3.3.4"

  // testing
  val scalaTest = "org.scalatest" %% "scalatest" % "3.0.7" % "test,it"
  val hadoopCommon =  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "it"

  // arc
  val arc = "ai.tripl" %% "arc" % "3.13.2" % "provided"

  // spark
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

  // parso does the actual parsing
  val parso = "com.epam" % "parso" % "2.0.11"

  // Project
  val etlDeps = Seq(
    scalaTest,
    hadoopCommon,

    arc,

    sparkSql,

    parso
  )
}