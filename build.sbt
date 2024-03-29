lazy val root = (project in file(".")).settings(
  organization := "com.tresata",
  name := "spark-datasetops",
  version := "1.7.0-SNAPSHOT",
  scalaVersion := "2.13.8",
  crossScalaVersions := Seq("2.12.17", "2.13.8"),
  Compile / compile / javacOptions ++= Seq("-Xlint:unchecked", "-source", "1.8", "-target", "1.8"),
  scalacOptions ++= Seq("-unchecked", "-deprecation", "-target:jvm-1.8", "-feature", "-language:_", "-Xlint:-package-object-classes,-adapted-args,_",
    "-Ywarn-unused:-imports", "-Ywarn-dead-code", "-Ywarn-value-discard", "-Ywarn-unused"),
  Test / compile / scalacOptions := (Test / compile / scalacOptions).value.filter(_ != "-Ywarn-value-discard").filter(_ != "-Ywarn-unused"),
  Compile / console / scalacOptions := (Compile / console / scalacOptions).value.filter(_ != "-Ywarn-unused-import"),
  Test / console / scalacOptions := (Test / console / scalacOptions).value.filter(_ != "-Ywarn-unused-import"),
  licenses += "Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"),
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",
    "org.scalatest" %% "scalatest-funspec" % "3.2.16" % "test"
  ),
  Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oF"),
  publishMavenStyle := true,
  pomIncludeRepository := { x => false },
  Test / publishArtifact := false,
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at "https://server02.tresata.com:8084/artifactory/oss-libs-snapshot-local")
    else
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
  },
  credentials += Credentials(Path.userHome / ".m2" / "credentials_sonatype"),
  credentials += Credentials(Path.userHome / ".m2" / "credentials_artifactory"),
  Global / useGpgPinentry := true,
  pomExtra := (
    <url>https://github.com/tresata/spark-datasetops</url>
    <scm>
      <url>git@github.com:tresata/spark-datasetops.git</url>
      <connection>scm:git:git@github.com:tresata/spark-datasetops.git</connection>
    </scm>
    <developers>
      <developer>
        <id>koertkuipers</id>
        <name>Koert Kuipers</name>
        <url>https://github.com/koertkuipers</url>
      </developer>
    </developers>)
)
