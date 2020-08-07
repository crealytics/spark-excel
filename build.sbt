name := "spark-excel"

organization := "com.crealytics"

enablePlugins(GitVersioning)

crossScalaVersions := Seq("2.12.10", "2.11.12")

scalaVersion := crossScalaVersions.value.head

spName := "crealytics/spark-excel"

sparkVersion := "2.4.4"

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.get("spark.testVersion").getOrElse(sparkVersion.value)

sparkComponents := Seq("core", "sql", "hive")

resolvers ++= Seq("jitpack" at "https://jitpack.io")

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.30" % "provided",
).map(_.excludeAll(ExclusionRule(organization = "stax")))

shadedDeps ++= Seq(
  "org.apache.poi" ^ "poi" ^ "4.1.2",
  "org.apache.poi" ^ "poi-ooxml" ^ "4.1.2",
  "com.norbitltd" ^^ "spoiwo" ^ "1.7.0",
  "com.github.pjfanning" ^ "excel-streaming-reader" ^ "2.3.4",
  "com.github.pjfanning" ^ "poi-shared-strings" ^ "1.0.4",
  "org.apache.commons" ^ "commons-compress" ^ "1.20",
  "com.fasterxml.jackson.core" ^ "jackson-core" ^ "2.8.8",
)

shadeRenames ++= Seq(
  "org.apache.poi.**" -> "shadeio.poi.@1",
  "com.norbitltd.spoiwo.**" -> "shadeio.spoiwo.@1",
  "com.github.pjfanning.**" -> "shadeio.pjfanning.@1",
  "com.fasterxml.jackson.**" -> "shadeio.jackson.@1",
  "org.apache.commons.compress.**" -> "shadeio.commons.compress.@1",
)

publishThinShadedJar

libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-core" % "2.0.0" % Test,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.11.2" % Test,
  "org.scalatest" %% "scalatest" % "3.2.1" % Test,
  "org.scalatestplus" %% "scalatestplus-scalacheck" % "3.1.0.0-RC2" % Test,
  "org.scalacheck" %% "scalacheck" % "1.14.3" % Test,
  "com.github.alexarchambault" %% "scalacheck-shapeless_1.14" % "1.2.5" % Test,
  "com.github.nightscape" %% "spark-testing-base" % "c2bc44caf4" % Test,
  //  "com.holdenkarau" %% "spark-testing-base" % s"${testSparkVersion.value}_0.7.4" % Test,
  "org.scalamock" %% "scalamock-scalatest-support" % "3.6.0" % Test
)


fork in Test := true
parallelExecution in Test := false
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

releaseCrossBuild := true
publishMavenStyle := true

spAppendScalaVersion := true

spIncludeMaven := true

publishTo := sonatypePublishToBundle.value

Global/useGpgPinentry := true

licenses += "Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")

homepage := Some(url("https://github.com/crealytics/spark-excel"))

developers ++= List(
  Developer("nightscape", "Martin Mauch", "@nightscape", url("https://github.com/nightscape"))
)
scmInfo := Some(ScmInfo(url("https://github.com/crealytics/spark-excel"), "git@github.com:crealytics/spark-excel.git"))

// Skip tests during assembly
test in assembly := {}

addArtifact(artifact in (Compile, assembly), assembly)

initialCommands in console := """
  import org.apache.spark.sql._
  val spark = SparkSession.
    builder().
    master("local[*]").
    appName("Console").
    config("spark.app.id", "Console").   // To silence Metrics warning.
    getOrCreate
  import spark.implicits._
  import org.apache.spark.sql.functions._    // for min, max, etc.
  import com.crealytics.spark.excel._
  """

fork := true

// -- MiMa binary compatibility checks ------------------------------------------------------------

mimaPreviousArtifacts := Set("com.crealytics" %% "spark-excel" % "0.0.1")
// ------------------------------------------------------------------------------------------------
