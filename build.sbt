name := "spark-excel"

organization := "com.crealytics"

crossScalaVersions := Seq("2.12.15")

scalaVersion := crossScalaVersions.value.head

lazy val sparkVersion = "3.1.2"

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.get("spark.testVersion").getOrElse(sparkVersion.value)

// For Spark DataSource API V2, spark-excel jar file depends on spark-version
version := testSparkVersion.value + "_" + version.value

resolvers ++= Seq("jitpack" at "https://jitpack.io")

libraryDependencies ++= Seq("org.slf4j" % "slf4j-api" % "1.7.32" % "provided")
  .map(_.excludeAll(ExclusionRule(organization = "stax")))

shadedDeps ++= Seq(
  "org.apache.poi" ^ "poi" ^ "5.0.0",
  "org.apache.poi" ^ "poi-ooxml" ^ "5.0.0",
  "com.norbitltd" ^^ "spoiwo" ^ "2.0.0",
  "com.github.pjfanning" ^ "excel-streaming-reader" ^ "3.2.1",
  "com.github.pjfanning" ^ "poi-shared-strings" ^ "2.1.1",
  "org.apache.commons" ^ "commons-compress" ^ "1.21"
)

shadeRenames ++= Seq(
  "org.apache.poi.**" -> "shadeio.poi.@1",
  "spoiwo.**" -> "shadeio.spoiwo.@1",
  "com.github.pjfanning.**" -> "shadeio.pjfanning.@1",
  "org.apache.commons.compress.**" -> "shadeio.commons.compress.@1"
)

publishThinShadedJar

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % testSparkVersion.value % "provided",
  "org.apache.spark" %% "spark-sql" % testSparkVersion.value % "provided",
  "org.apache.spark" %% "spark-hive" % testSparkVersion.value % "provided",
  "org.typelevel" %% "cats-core" % "2.6.1" % Test,
  "org.scalatest" %% "scalatest" % "3.2.9" % Test,
  "org.scalatestplus" %% "scalacheck-1-15" % "3.2.10.0" % Test,
  "org.scalacheck" %% "scalacheck" % "1.15.4" % Test,
  "com.github.alexarchambault" %% "scalacheck-shapeless_1.15" % "1.3.0" % Test,
  "com.github.nightscape" %% "spark-testing-base" % "c2bc44caf4" % Test,
  //  "com.holdenkarau" %% "spark-testing-base" % s"${testSparkVersion.value}_0.7.4" % Test,
  "org.scalamock" %% "scalamock-scalatest-support" % "3.6.0" % Test
)

// Custom source layout for Spark Data Source API 2
Compile / unmanagedSourceDirectories := {
  if (testSparkVersion.value >= "3.1.0")
    Seq(
      (Compile / sourceDirectory)(_ / "scala"),
      (Compile / sourceDirectory)(_ / "3.x/scala"),
      (Compile / sourceDirectory)(_ / "3.1/scala")
    ).join.value
  else if (testSparkVersion.value >= "3.0.0")
    Seq(
      (Compile / sourceDirectory)(_ / "scala"),
      (Compile / sourceDirectory)(_ / "3.x/scala"),
      (Compile / sourceDirectory)(_ / "3.0/scala")
    ).join.value
  else if (testSparkVersion.value >= "2.4.0")
    Seq((Compile / sourceDirectory)(_ / "scala"), (Compile / sourceDirectory)(_ / "2.4/scala")).join.value
  else throw new UnsupportedOperationException(s"testSparkVersion ${testSparkVersion.value} is not supported")
}

Test / fork := true
Test / parallelExecution := false
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

publishMavenStyle := true

publishTo := sonatypePublishToBundle.value

Global / useGpgPinentry := true

licenses += "Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")

homepage := Some(url("https://github.com/crealytics/spark-excel"))

developers ++= List(Developer("nightscape", "Martin Mauch", "@nightscape", url("https://github.com/nightscape")))
scmInfo := Some(ScmInfo(url("https://github.com/crealytics/spark-excel"), "git@github.com:crealytics/spark-excel.git"))

// Skip tests during assembly
assembly / test := {}

addArtifact(Compile / assembly / artifact, assembly)

console / initialCommands := """
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
