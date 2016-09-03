name := "spark-excel"

version := "0.8.2"

organization := "com.crealytics"

scalaVersion := "2.11.8"

spName := "crealytics/spark-excel"

crossScalaVersions := Seq("2.10.6", "2.11.8")

sparkVersion := "2.0.0"

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.get("spark.testVersion").getOrElse(sparkVersion.value)

sparkComponents := Seq("core", "sql")

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.5" % "provided",
  "org.apache.poi" % "poi-ooxml" % "3.14"
)

publishMavenStyle := true

spAppendScalaVersion := true

spIncludeMaven := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (version.value.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

pomExtra :=
  <url>https://github.com/crealytics/spark-excel</url>
    <licenses>
      <license>
        <name>Apache License, Version 2.0</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <url>git@github.com:crealytics/spark-excel.git</url>
      <connection>scm:git:git@github.com:crealytics/spark-excel.git</connection>
    </scm>
    <developers>
      <developer>
        <id>nightscape</id>
        <name>Martin Mauch</name>
        <url>http://www.crealytics.com</url>
      </developer>
    </developers>

// Skip tests during assembly
test in assembly := {}

addArtifact(artifact in(Compile, assembly), assembly)


initialCommands += """
  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark.SparkContext._
  import org.apache.spark.sql.SQLContext
  val conf = new SparkConf().
    setMaster("local[*]").
    setAppName("Console").
    set("spark.app.id", "Console")   // To silence Metrics warning.
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  import org.apache.spark.sql.functions._    // for min, max, etc.
  """

fork := true

// -- MiMa binary compatibility checks ------------------------------------------------------------

import com.typesafe.tools.mima.plugin.MimaKeys.{binaryIssueFilters, previousArtifact}
import com.typesafe.tools.mima.plugin.MimaPlugin.mimaDefaultSettings

mimaDefaultSettings ++ Seq(
  previousArtifact := Some("com.crealytics" %% "spark-excel" % "0.0.1"),
  binaryIssueFilters ++= Seq(
  )
)
// ------------------------------------------------------------------------------------------------
