name := "spark-excel"

organization := "com.crealytics"

crossScalaVersions := Seq("2.11.12", "2.10.6")

scalaVersion := crossScalaVersions.value.head

spName := "crealytics/spark-excel"

sparkVersion := "2.3.1"

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.get("spark.testVersion").getOrElse(sparkVersion.value)

sparkComponents := Seq("core", "sql", "hive")

resolvers ++= Seq("jitpack" at "https://jitpack.io")

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.5" % "provided",
  "com.norbitltd" %% "spoiwo" % "1.4.0",
  "com.monitorjbl" % "xlsx-streamer" % "2.0.0"
).map(_.excludeAll(ExclusionRule(organization = "stax")))

shadedDeps ++= Seq(
  "org.apache.poi" ^ "poi-ooxml" ^ "4.0.0",
  "org.apache.commons" ^ "commons-compress" ^ "1.18",
  "com.fasterxml.jackson.core" ^ "jackson-core" ^ "2.8.8",
)

shadeRenames ++= Seq(
  "com.fasterxml.jackson.**" -> "shadeio.jackson.@1",
  "org.apache.commons.compress.**" -> "shadeio.commons.compress.@1",
)

publishThinShadedJar

libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-core" % "1.4.0" % Test,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.8.8" % Test,
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "org.scalacheck" %% "scalacheck" % "1.14.0" % Test,
  "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % "1.1.8" % Test,
  "com.github.nightscape" % "spark-testing-base" % "c6ac5d3b0629440f5fe13cf8830fdb17535c8513" % Test,
//  "com.holdenkarau" %% "spark-testing-base" % s"${testSparkVersion.value}_0.7.4" % Test,
  "org.scalamock" %% "scalamock-scalatest-support" % "3.6.0" % Test
)


fork in Test := true
parallelExecution in Test := false
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

publishMavenStyle := true

spAppendScalaVersion := true

spIncludeMaven := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (version.value.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
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

addArtifact(artifact in (Compile, assembly), assembly)

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

mimaPreviousArtifacts := Set("com.crealytics" %% "spark-excel" % "0.0.1")
// ------------------------------------------------------------------------------------------------
