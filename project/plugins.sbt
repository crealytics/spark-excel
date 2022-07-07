resolvers += Resolver.bintrayIvyRepo("s22s", "sbt-plugins")
addSbtPlugin("org.typelevel" % "sbt-typelevel" % "0.4.12")
addSbtPlugin(
  "org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0" excludeAll (ExclusionRule(organization = "com.danieltrinh"))
)
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.2.0")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.6")
