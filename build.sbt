Global / onChangedBuildSource := ReloadOnSourceChanges

val sparkVersion = "3.5.0"
val sparkMajorVersion = sparkVersion.substring(0, sparkVersion.lastIndexOf("."))

organization := "com.amazon.ion"
name := s"ion-spark-data-source-$sparkMajorVersion"
versionScheme := Some("semver-spec")

scalaVersion := "2.12.17"
scalacOptions ++= Seq("-target:jvm-1.8", "-Xexperimental", "-deprecation", "-Xfuture")
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

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
  "-Dlog4j2.configurationFile=tst/log4j2.properties"
)

coverageFailOnMinimum := true
coverageMinimumStmtTotal := 80
coverageMinimumBranchTotal := 80

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "com.amazon.ion" % "ion-java" % "1.+",
  "com.amazon.ion" % "ion-java-path-extraction" % "1.+",
  "org.apache.hadoop" % "hadoop-client" % "3.3.+" % Provided,
  "org.scalatest" %% "scalatest" % "3.2.+" % Test,
  "org.scalatestplus" %% "mockito-3-4" % "3.2.+" % Test,
  "org.scalatestplus" %% "scalacheck-1-17" % "3.2.+" % Test,
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
