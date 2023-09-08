import $ivy.`io.chris-kipp::mill-ci-release::0.1.9`
import io.kipp.mill.ci.release.CiReleaseModule
import coursier.maven.MavenRepository
import mill._, scalalib._, publish._
import mill.modules.Assembly._

trait SparkModule extends Cross.Module2[String, String] with SbtModule with CiReleaseModule {
  outer =>
  override def scalaVersion = crossValue
  val sparkVersion = crossValue2

  override def millSourcePath = super.millSourcePath / os.up / os.up / os.up

  // Custom source layout for Spark Data Source API 2
  val sparkVersionSpecificSources = if (sparkVersion >= "3.4.0") {
    Seq("scala", "3.0_and_up/scala", "3.1_and_up/scala", "3.2_and_up/scala", "3.3_and_up/scala", "3.4_and_up/scala")
  } else if (sparkVersion >= "3.3.0") {
    Seq(
      "scala",
      "3.0_3.1_3.2_3.3/scala",
      "3.0_and_up/scala",
      "3.1_and_up/scala",
      "3.2_and_up/scala",
      "3.3_and_up/scala"
    )
  } else if (sparkVersion >= "3.2.0") {
    Seq(
      "scala",
      "3.0_3.1_3.2/scala",
      "3.0_3.1_3.2_3.3/scala",
      "3.0_and_up/scala",
      "3.1_and_up/scala",
      "3.2_and_up/scala"
    )
  } else if (sparkVersion >= "3.1.0") {
    Seq(
      "scala",
      "3.1/scala",
      "3.0_3.1/scala",
      "3.0_3.1_3.2_3.3/scala",
      "3.0_3.1_3.2/scala",
      "3.0_and_up/scala",
      "3.1_and_up/scala"
    )
  } else if (sparkVersion >= "3.0.0") {
    Seq("scala", "3.0/scala", "3.0_3.1/scala", "3.0_3.1_3.2_3.3/scala", "3.0_3.1_3.2/scala", "3.0_and_up/scala")
  } else if (sparkVersion >= "2.4.0") {
    Seq("scala", "2.4/scala")
  } else {
    throw new UnsupportedOperationException(s"sparkVersion ${sparkVersion} is not supported")
  }

  override def sources = T.sources {
    super.sources() ++ sparkVersionSpecificSources.map(s => PathRef(millSourcePath / "src" / "main" / os.RelPath(s)))
  }

  override def docSources = T.sources(Seq[PathRef]())

  override def artifactName = "spark-excel"

  override def publishVersion = s"${sparkVersion}_${super.publishVersion()}"

  def pomSettings = PomSettings(
    description = "A Spark plugin for reading and writing Excel files",
    organization = "com.crealytics",
    url = "https://github.com/crealytics/spark-excel",
    licenses = Seq(License.`Apache-2.0`),
    versionControl = VersionControl.github("crealytics", "spark-excel"),
    developers = Seq(Developer("nightscape", "Martin Mauch", "https://github.com/nightscape"))
  )

  def assemblyRules = Seq(
    Rule.AppendPattern(".*\\.conf"), // all *.conf files will be concatenated into single file
    Rule.Relocate("org.apache.commons.io.**", "shadeio.commons.io.@1"),
    Rule.Relocate("org.apache.commons.compress.**", "shadeio.commons.compress.@1")
  )

  override def extraPublish = Seq(PublishInfo(assembly(), classifier = None, ivyConfig = "compile"))

  val sparkDeps = Agg(
    ivy"org.apache.spark::spark-core:$sparkVersion",
    ivy"org.apache.spark::spark-sql:$sparkVersion",
    ivy"org.apache.spark::spark-hive:$sparkVersion"
  )

  override def compileIvyDeps = if (sparkVersion < "3.3.0") {
    sparkDeps ++ Agg(ivy"org.slf4j:slf4j-api:1.7.36".excludeOrg("stax"))
  } else {
    sparkDeps
  }

  val poiVersion = "5.2.3"

  override def ivyDeps = {
    val base = Agg(
      ivy"org.apache.poi:poi:$poiVersion",
      ivy"org.apache.poi:poi-ooxml:$poiVersion",
      ivy"org.apache.poi:poi-ooxml-lite:$poiVersion",
      ivy"org.apache.xmlbeans:xmlbeans:5.1.1",
      ivy"com.norbitltd::spoiwo:2.2.1",
      ivy"com.github.pjfanning:excel-streaming-reader:4.1.1",
      ivy"com.github.pjfanning:poi-shared-strings:2.6.1",
      ivy"commons-io:commons-io:2.13.0",
      ivy"org.apache.commons:commons-compress:1.24.0",
      ivy"org.apache.logging.log4j:log4j-api:2.20.0",
      ivy"com.zaxxer:SparseBitSet:1.3",
      ivy"org.apache.commons:commons-collections4:4.4",
      ivy"com.github.virtuald:curvesapi:1.08",
      ivy"commons-codec:commons-codec:1.16.0",
      ivy"org.apache.commons:commons-math3:3.6.1",
      ivy"org.scala-lang.modules::scala-collection-compat:2.11.0"
    )
    if (sparkVersion >= "3.3.0") {
      base ++ Agg(ivy"org.apache.logging.log4j:log4j-core:2.20.0")
    } else {
      base
    }
  }

  object test extends SbtModuleTests with TestModule.ScalaTest {

    override def millSourcePath = super.millSourcePath

    override def sources = T.sources {
      Seq(PathRef(millSourcePath / "src" / "test" / "scala"))
    }

    override def resources = T.sources {
      Seq(PathRef(millSourcePath / "src" / "test" / "resources"))
    }

    def scalaVersion = outer.scalaVersion()

    def repositoriesTask = T.task {
      super.repositoriesTask() ++ Seq(MavenRepository("https://jitpack.io"))
    }

    def ivyDeps = sparkDeps ++ Agg(
      ivy"org.typelevel::cats-core:2.10.0",
      ivy"org.scalatest::scalatest:3.2.17",
      ivy"org.scalatestplus::scalacheck-1-16:3.2.14.0",
      ivy"org.scalacheck::scalacheck:1.17.0",
      ivy"com.github.alexarchambault::scalacheck-shapeless_1.15:1.3.0",
      ivy"com.github.mrpowers::spark-fast-tests:1.3.0",
      ivy"org.scalamock::scalamock:5.2.0"
    )
  }
}

val scala213 = "2.13.11"
val scala212 = "2.12.18"
val spark24 = List("2.4.1", "2.4.7", "2.4.8")
val spark30 = List("3.0.1", "3.0.3")
val spark31 = List("3.1.1", "3.1.2", "3.1.3")
val spark32 = List("3.2.4")
val spark33 = List("3.3.3")
val spark34 = List("3.4.1")

val crossMatrix = {

  (spark24 ++ spark30 ++ spark31 ++ spark32 ++ spark33 ++ spark34).map(spark =>
    (scala212, spark)
  ) ++ (spark32 ++ spark33 ++ spark34).map(spark => (scala213, spark))

  //  (spark34).map(spark => (scala212, spark))
}

object `spark-excel` extends Cross[SparkModule](crossMatrix) {}
