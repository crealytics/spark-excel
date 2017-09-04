releasePublishArtifactsAction := PgpKeys.publishSigned.value
releaseProcess ++= Seq[ReleaseStep](
  releaseStepCommand("sonatypeRelease")
)
