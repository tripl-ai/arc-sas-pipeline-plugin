import sbt._

object Dependencies {
  // versions
  lazy val sparkVersion = "2.4.5"
  lazy val hadoopVersion = "2.9.2"

  // testing
  val scalaTest = "org.scalatest" %% "scalatest" % "3.0.7" % "test,it"
  val hadoopCommon =  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "it"

  // arc
  val arc = "ai.tripl" %% "arc" % "2.10.0" % "provided"
  val typesafeConfig = "com.typesafe" % "config" % "1.3.1" intransitive()  

  // spark
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

  // parso does the actual parsing
  val parso = "com.epam" % "parso" % "2.0.11"

  // Project
  val etlDeps = Seq(
    scalaTest,
    hadoopCommon,
    
    arc,
    typesafeConfig,

    sparkSql,

    parso
  )
}