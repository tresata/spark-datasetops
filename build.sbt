lazy val root = (project in file(".")).settings(
  organization := "com.tresata",
  name := "spark-datasetops",
  version := "0.6.0-SNAPSHOT",
  scalaVersion := "2.11.12",
  crossScalaVersions := Seq("2.11.12", "2.12.8"),
  javacOptions in (Compile, compile) ++= Seq("-Xlint:unchecked", "-source", "1.8", "-target", "1.8"),
  scalacOptions ++= Seq("-unchecked", "-deprecation", "-target:jvm-1.8", "-feature", "-language:_", "-Xlint:-package-object-classes,-adapted-args,_",
    "-Ywarn-unused-import", "-Ywarn-dead-code", "-Ywarn-value-discard", "-Ywarn-unused"),
  scalacOptions in (Test, compile) := (scalacOptions in (Test, compile)).value.filter(_ != "-Ywarn-value-discard").filter(_ != "-Ywarn-unused"),
  scalacOptions in (Compile, console) := (scalacOptions in (Compile, console)).value.filter(_ != "-Ywarn-unused-import"),
  scalacOptions in (Test, console) := (scalacOptions in (Test, console)).value.filter(_ != "-Ywarn-unused-import"),
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",
    "org.scalatest" %% "scalatest" % "3.0.6" % "test"
  ),
  testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oF"),
  publishMavenStyle := true,
  pomIncludeRepository := { x => false },
  publishArtifact in Test := false,
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at "http://server02.tresata.com:8081/artifactory/oss-libs-snapshot-local")
    else
      Some("releases"  at nexus + "service/local/staging/deploy/maven2")
  },
  credentials += Credentials(Path.userHome / ".m2" / "credentials_sonatype"),
  credentials += Credentials(Path.userHome / ".m2" / "credentials_artifactory"),
  pomExtra := (
    <url>https://github.com/tresata/spark-datasetops</url>
    <licenses>
      <license>
      <name>Apache 2</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      <distribution>repo</distribution>
      <comments>A business-friendly OSS license</comments>
      </license>
    </licenses>
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
    </developers>
  )
)
