
scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.10.4", "2.11.8")

parallelExecution in Test := false

val nscalaVersion = "2.12.0"
val scalatestVersion = "2.2.4"
val sparkVersion = "2.0.0"

val coreDependencies = Seq(
  "com.github.nscala-time" %% "nscala-time" % nscalaVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)

val coreTestDependencies = Seq(
  "org.scalatest" %% "scalatest" % scalatestVersion % "test"
)

lazy val commonSettings = Seq(
  organization := "com.sparklinedata",

  version := "0.0.3",

  javaOptions := Seq("-Xms512m", "-Xmx512m", "-XX:MaxPermSize=256M"),

  // Target Java 7
  scalacOptions += "-target:jvm-1.8",
  javacOptions in compile ++= Seq("-source", "1.8", "-target", "1.8"),

  scalacOptions := Seq("-feature", "-deprecation"),

  licenses := Seq("Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),

  homepage := Some(url("https://github.com/SparklineData/spark-datetime")),

  publishMavenStyle := true,

  publishTo := Some("releases" at "https://oss.sonatype.org/service/local/staging/deploy/maven2/"),

  publishArtifact in Test := false,

  pomIncludeRepository := { _ => false },

  test in assembly := {},

  useGpg := true,

  usePgpKeyHex("C922EB45"),

  pomExtra := (
    <scm>
      <url>https://github.com/SparklineData/spark-datetime.git</url>
      <connection>scm:git:git@github.com:SparklineData/spark-datetime.git</connection>
    </scm>
      <developers>
        <developer>
          <name>Harish Butani</name>
          <organization>SparklineData</organization>
          <organizationUrl>http://sparklinedata.com/</organizationUrl>
        </developer>
      </developers>),

  fork in Test := true
) ++ releaseSettings ++ net.virtualvoid.sbt.graph.Plugin.graphSettings ++ Seq(
  ReleaseKeys.publishArtifactsAction := PgpKeys.publishSigned.value
)


lazy val root = project.in(file("."))
  .settings(commonSettings: _*)
  .settings(name := "spark-datetime")
  .settings(libraryDependencies ++= (coreDependencies ++ coreTestDependencies))
  .settings(assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false))
  .settings(
  artifact in (Compile, assembly) ~= { art =>
    art.copy(`classifier` = Some("assembly"))
  },
    assemblyExcludedJars in assembly := {
      val cp = (fullClasspath in assembly).value
      println(cp)
      cp filter {_.data.getName startsWith "joda"}
    }
  )
  .settings(addArtifact(artifact in (Compile, assembly), assembly).settings: _*)
