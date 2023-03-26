resolvers += Resolver.bintrayIvyRepo("s22s", "sbt-plugins")
addSbtPlugin("org.typelevel" % "sbt-typelevel" % "0.4.14")
addSbtPlugin(
  "org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0" excludeAll (ExclusionRule(organization = "com.danieltrinh"))
)
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.1")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.0")
addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings" % "3.0.2")
addDependencyTreePlugin
