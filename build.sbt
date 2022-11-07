name := "spark-excel"

val scala213 = "2.13.10"
val scala212 = "2.12.17"
val spark24 = List("2.4.1", "2.4.7", "2.4.8")
val spark30 = List("3.0.1", "3.0.3")
val spark31 = List("3.1.1", "3.1.2", "3.1.3")
val spark32 = List("3.2.2")
inThisBuild(
  List(
    organization := "com.crealytics",
    organizationName := "Martin Mauch (@nightscape)",
    homepage := Some(url("https://github.com/crealytics/spark-excel")),
    licenses := List(License.Apache2),
    tlBaseVersion := "0.18",
    crossScalaVersions := Seq(scala212, scala213),
    scalaVersion := crossScalaVersions.value.head,
    scalacOptions += "-Wconf:origin=scala.collection.compat.*:s",
    githubWorkflowBuildMatrixFailFast := Some(false),
    githubWorkflowBuildMatrixAdditions := Map("spark" -> (spark24 ++ spark30 ++ spark31 ++ spark32)),
    githubWorkflowBuildMatrixExclusions := (spark24 ++ spark30 ++ spark31).map(spark =>
      MatrixExclude(Map("spark" -> spark, "scala" -> scala213))
    ),
    githubWorkflowBuildSbtStepPreamble := Seq("-Dspark.testVersion=${{ matrix.spark }}", "++${{ matrix.scala }}"),
    githubWorkflowBuild := Seq(
      WorkflowStep.Sbt(List("test"), name = Some("Test")),
      WorkflowStep.Sbt(List("assembly"), name = Some("Assembly")),
      WorkflowStep.Sbt(List("scalastyle", "test:scalastyle"), name = Some("Scalastyle"))
    ),
    githubWorkflowPublish := Seq(WorkflowStep.Sbt(List("tlRelease"), name = Some("Publish"))),
    tlSonatypeUseLegacyHost := true,
    autoAPIMappings := true,
    developers := List(
      Developer("nightscape", "Martin Mauch", "martin.mauch@gmail.com", url("https://github.com/nightscape"))
    )
  )
)

lazy val sparkVersion = "3.2.2"
val poiVersion = "5.2.3"

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.get("spark.testVersion").getOrElse(sparkVersion)

// For Spark DataSource API V2, spark-excel jar file depends on spark-version
version := testSparkVersion.value + "_" + version.value

resolvers ++= Seq("jitpack" at "https://jitpack.io")

libraryDependencies ++= Seq("org.slf4j" % "slf4j-api" % "1.7.36" % "provided")
  .map(_.excludeAll(ExclusionRule(organization = "stax")))

enablePlugins(ThinFatJar)
shadedDeps ++= Seq(
  "org.apache.poi" % "poi" % poiVersion,
  "org.apache.poi" % "poi-ooxml" % poiVersion,
  "org.apache.poi" % "poi-ooxml-lite" % poiVersion,
  "org.apache.xmlbeans" % "xmlbeans" % "5.1.1",
  "com.norbitltd" %% "spoiwo" % "2.2.1",
  "com.github.pjfanning" % "excel-streaming-reader" % "4.0.4",
  "com.github.pjfanning" % "poi-shared-strings" % "2.5.5",
  "commons-io" % "commons-io" % "2.11.0",
  "org.apache.commons" % "commons-compress" % "1.22",
  "org.apache.logging.log4j" % "log4j-api" % "2.19.0",
  "com.zaxxer" % "SparseBitSet" % "1.2",
  "org.apache.commons" % "commons-collections4" % "4.4",
  "com.github.virtuald" % "curvesapi" % "1.07",
  "commons-codec" % "commons-codec" % "1.15",
  "org.apache.commons" % "commons-math3" % "3.6.1"
)

shadeRenames ++= Seq(
  "org.apache.commons.io.**" -> "shadeio.commons.io.@1",
  "org.apache.commons.compress.**" -> "shadeio.commons.compress.@1"
)

publishThinShadedJar

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % testSparkVersion.value % "provided",
  "org.apache.spark" %% "spark-sql" % testSparkVersion.value % "provided",
  "org.apache.spark" %% "spark-hive" % testSparkVersion.value % "provided",
  "org.scala-lang.modules" %% "scala-collection-compat" % "2.8.1",
  "org.typelevel" %% "cats-core" % "2.8.0" % Test,
  "org.scalatest" %% "scalatest" % "3.2.14" % Test,
  "org.scalatestplus" %% "scalacheck-1-15" % "3.2.11.0" % Test,
  "org.scalacheck" %% "scalacheck" % "1.17.0" % Test,
  "com.github.alexarchambault" %% "scalacheck-shapeless_1.15" % "1.3.0" % Test,
  "com.github.mrpowers" %% "spark-fast-tests" % "1.3.0" % Test,
  "org.scalamock" %% "scalamock" % "5.2.0" % Test
)

// Custom source layout for Spark Data Source API 2
Compile / unmanagedSourceDirectories := {
  if (testSparkVersion.value >= "3.2.2") {
    Seq(
      (Compile / sourceDirectory)(_ / "scala"),
      (Compile / sourceDirectory)(_ / "3.x/scala"),
      (Compile / sourceDirectory)(_ / "3.1_3.2/scala"),
      (Compile / sourceDirectory)(_ / "3.2/scala")
    ).join.value
  } else if (testSparkVersion.value >= "3.1.0") {
    Seq(
      (Compile / sourceDirectory)(_ / "scala"),
      (Compile / sourceDirectory)(_ / "3.x/scala"),
      (Compile / sourceDirectory)(_ / "3.0_3.1/scala"),
      (Compile / sourceDirectory)(_ / "3.1/scala"),
      (Compile / sourceDirectory)(_ / "3.1_3.2/scala")
    ).join.value
  } else if (testSparkVersion.value >= "3.0.0") {
    Seq(
      (Compile / sourceDirectory)(_ / "scala"),
      (Compile / sourceDirectory)(_ / "3.x/scala"),
      (Compile / sourceDirectory)(_ / "3.0/scala"),
      (Compile / sourceDirectory)(_ / "3.0_3.1/scala")
    ).join.value
  } else if (testSparkVersion.value >= "2.4.0") {
    Seq((Compile / sourceDirectory)(_ / "scala"), (Compile / sourceDirectory)(_ / "2.4/scala")).join.value
  } else {
    throw new UnsupportedOperationException(s"testSparkVersion ${testSparkVersion.value} is not supported")
  }
}

Test / fork := true
Test / parallelExecution := false
javaOptions ++= Seq("-Xms512M", "-Xmx2048M")

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

mimaPreviousArtifacts := Set.empty
// ------------------------------------------------------------------------------------------------
