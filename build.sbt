name := "spark-excel"

version := "0.8.4"

organization := "com.crealytics"

crossScalaVersions := Seq("2.11.11", "2.10.6")

scalaVersion := crossScalaVersions.value.head

spName := "crealytics/spark-excel"

sparkVersion := "2.1.1"

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.get("spark.testVersion").getOrElse(sparkVersion.value)

sparkComponents := Seq("core", "sql")

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.5" % "provided",
  "org.apache.poi" % "poi-ooxml" % "3.16"
)

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.1" % Test,
  "org.scalacheck" %% "scalacheck" % "1.13.4" % Test,
  "org.scalamock" %% "scalamock-scalatest-support" % "3.5.0" % Test
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
