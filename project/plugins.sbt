resolvers += Resolver.bintrayIvyRepo("s22s", "sbt-plugins")

addSbtPlugin(
  "org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0" excludeAll (ExclusionRule(organization = "com.danieltrinh"))
)

addSbtPlugin("com.github.sbt" % "sbt-unidoc" % "0.5.0")

addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "1.0.1")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.1.0")

addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.5.10")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.6")
