Global / onChangedBuildSource := ReloadOnSourceChanges

val sparkVersion = "4.0.1"
val sparkMajorVersion = sparkVersion.substring(0, sparkVersion.lastIndexOf("."))

organization := "com.amazon.ion"
organizationHomepage := Some(url("https://amazon-ion.github.io/ion-docs/"))
name := s"ion-spark-data-source-$sparkMajorVersion"
homepage := Some(url("https://github.com/amazon-ion/ion-spark-data-source"))
licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0"))
developers := List(
  Developer(
    "ion-team",
    "Amazon Ion Team",
    "ion-team@amazon.com",
    url("https://github.com/amazon-ion")
  )
)

// force the artifact name in maven to keep the . in Spark version rather than substitute it with a -
moduleName := name.value
versionScheme := Some("semver-spec")

scalaVersion := "2.13.14"
scalacOptions ++= Seq("-release:17", "-deprecation", "-feature", "-unchecked")
javacOptions ++= Seq("-source", "17", "-target", "17")

Compile / scalaSource := baseDirectory.value / "src"
Compile / unmanagedResourceDirectories += baseDirectory.value / "src" / "resources"
Test / scalaSource := baseDirectory.value / "tst"
Test / fork := true
Test / javaOptions ++= Seq(
  "--add-opens",
  "java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens",
  "java.base/java.nio=ALL-UNNAMED",
  "--add-opens",
  "java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-opens",
  "java.base/java.lang=ALL-UNNAMED",
  "--add-opens",
  "java.base/java.util=ALL-UNNAMED",
  "-Djava.security.manager=allow",
  "-Dlog4j2.configurationFile=tst/log4j2.properties"
)

coverageFailOnMinimum := true
coverageMinimumStmtTotal := 80
coverageMinimumBranchTotal := 80

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "com.amazon.ion" % "ion-java" % "1.+",
  "com.amazon.ion" % "ion-java-path-extraction" % "1.+",
  "org.apache.hadoop" % "hadoop-client" % "3.4.+" % Provided,
  "org.scalatest" %% "scalatest" % "3.2.+" % Test,
  "org.scalatestplus" %% "mockito-5-12" % "3.2.+" % Test,
  "org.scalatestplus" %% "scalacheck-1-18" % "3.2.+" % Test,
  // for HTML test reports
  "com.vladsch.flexmark" % "flexmark-all" % "0.64.8" % Test
)

// disable parallel tests because we run Spark
Test / parallelExecution := false
Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-h", "target/test-reports")

addCommandAlias("release", releaseTasks("publish"))
addCommandAlias("releaseLocal", releaseTasks("publishLocal"))

publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true)
publishConfiguration := publishConfiguration.value.withOverwrite(true)
pgpSigningKey := sys.env.get("PGP_SIGNING_KEY_ID")

// NOTE: CI release escape hatch: If a release needs to be manually pushed to maven central,
// specify the options below. Ensure the version is correct, and run `sbt ci-release` locally
// with the same environment variables needed for the CI build. This will circumvent any 'SNAPSHOT'
// detection, and issues with the sbt-ci-release plugin not pushing releases outside of a CI env.
// isSnapshot := false
// version := "1.0.0"

def releaseTasks(publishTask: String) = {
  val tasks = Seq(
    "scalafmtCheckAll",
    "coverageOn",
    "+test",
    "coverageReport",
    "coverageOff",
    s"+$publishTask"
  )

  tasks.mkString("; ")
}
