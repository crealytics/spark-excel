resolvers += Resolver.bintrayIvyRepo("s22s", "sbt-plugins")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0" excludeAll(
  ExclusionRule(organization = "com.danieltrinh")))


addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.4.3")

addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "0.9.1")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.15.0")

addSbtPlugin("org.hammerlab.sbt" % "assembly" % "5.0.0")

addSbtPlugin("com.geirsson" % "sbt-ci-release" % "1.5.7")

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.9.7")

addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.1.2")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.2")
