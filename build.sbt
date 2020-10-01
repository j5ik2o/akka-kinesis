val scala211Version = "2.11.12"
val scala212Version = "2.12.10"
val scala213Version = "2.13.1"

def crossScalacOptions(scalaVersion: String): Seq[String] = CrossVersion.partialVersion(scalaVersion) match {
  case Some((2L, scalaMajor)) if scalaMajor >= 12 =>
    Seq.empty
  case Some((2L, scalaMajor)) if scalaMajor <= 11 =>
    Seq("-Yinline-warnings")
}

lazy val deploySettings = Seq(
  sonatypeProfileName := "com.github.j5ik2o",
  publishMavenStyle := true,
  publishArtifact in Test := false,
  pomIncludeRepository := { _ => false },
  pomExtra := {
    <url>https://github.com/j5ik2o/akka-kinesis</url>
      <licenses>
        <license>
          <name>Apache 2</name>
          <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
        </license>
      </licenses>
      <scm>
        <url>git@github.com:j5ik2o/akka-kinesis.git</url>
        <connection>scm:git:github.com/j5ik2o/akka-kinesis</connection>
        <developerConnection>scm:git:git@github.com:j5ik2o/akka-kinesis.git</developerConnection>
      </scm>
      <developers>
        <developer>
          <id>j5ik2o</id>
          <name>Junichi Kato</name>
        </developer>
      </developers>
  },
  publishTo := sonatypePublishToBundle.value,
  credentials := {
    val ivyCredentials = (baseDirectory in LocalRootProject).value / ".credentials"
    val gpgCredentials = (baseDirectory in LocalRootProject).value / ".gpgCredentials"
    Credentials(ivyCredentials) :: Credentials(gpgCredentials) :: Nil
  }
)

lazy val baseSettings = Seq(
  organization := "com.github.j5ik2o",
  scalaVersion := scala212Version,
  crossScalaVersions := Seq(scala211Version, scala212Version, scala213Version),
  scalacOptions ++= (Seq(
      "-feature",
      "-deprecation",
      "-unchecked",
      "-encoding",
      "UTF-8",
      "-language:_",
      "-Ydelambdafy:method",
      "-target:jvm-1.8"
    ) ++ crossScalacOptions(scalaVersion.value)),
  resolvers ++= Seq(
      Resolver.sonatypeRepo("snapshots"),
      Resolver.sonatypeRepo("releases"),
      "Seasar Repository" at "https://maven.seasar.org/maven2/",
      "DynamoDB Local Repository" at "https://s3-us-west-2.amazonaws.com/dynamodb-local/release"
    ),
  parallelExecution in Test := false,
  scalafmtOnCompile in ThisBuild := true,
  envVars := Map(
      "AWS_REGION" -> "ap-northeast-1"
    )
)

val awsSdkVersion              = "1.11.788"
val akkaVersion                = "2.5.31"
val testcontainersScalaVersion = "0.36.1"

val dependenciesCommonSettings = Seq(
  resolvers ++= Seq(
      "Sonatype OSS Snapshot Repository" at "https://oss.sonatype.org/content/repositories/snapshots/",
      "Sonatype OSS Release Repository" at "https://oss.sonatype.org/content/repositories/releases/",
      Resolver.bintrayRepo("hseeberger", "maven")
    ),
  libraryDependencies ++= Seq(
      "org.scalatest"     %% "scalatest"                       % "3.1.2" % Test,
      "org.scalacheck"    %% "scalacheck"                      % "1.14.3" % Test,
      "com.typesafe"      % "config"                           % "1.4.0",
      "ch.qos.logback"    % "logback-classic"                  % "1.2.3",
      "com.typesafe.akka" %% "akka-slf4j"                      % akkaVersion,
      "com.typesafe.akka" %% "akka-testkit"                    % akkaVersion % Test,
      "com.typesafe.akka" %% "akka-stream"                     % akkaVersion,
      "com.typesafe.akka" %% "akka-stream-testkit"             % akkaVersion % Test,
      "com.amazonaws"     % "aws-java-sdk-core"                % awsSdkVersion,
      "com.amazonaws"     % "aws-java-sdk-kinesis"             % awsSdkVersion,
      "com.dimafeng"      %% "testcontainers-scala-scalatest"  % testcontainersScalaVersion,
      "com.dimafeng"      %% "testcontainers-scala-localstack" % testcontainersScalaVersion
    ),
  Test / fork := true,
  envVars in Test := Map("AWS_CBOR_DISABLE" -> "1")
)

val `akka-kinesis-kpl` = (project in file("akka-kinesis-kpl"))
  .settings(baseSettings, dependenciesCommonSettings)
  .settings(
    name := "akka-kinesis-kpl",
    libraryDependencies ++= Seq(
        "com.amazonaws" % "amazon-kinesis-producer" % "0.14.1",
        "com.amazonaws" % "aws-java-sdk-cloudwatch" % awsSdkVersion % Test,
        "com.amazonaws" % "aws-java-sdk-dynamodb"   % awsSdkVersion % Test
      ),
    parallelExecution in Test := false
  )

val `akka-kinesis-kcl` = (project in file("akka-kinesis-kcl"))
  .settings(baseSettings, dependenciesCommonSettings)
  .settings(
    name := "akka-kinesis-kcl",
    libraryDependencies ++= Seq(
        "com.amazonaws"          % "amazon-kinesis-client"   % "1.11.2",
        "com.amazonaws"          % "aws-java-sdk-cloudwatch" % awsSdkVersion % Test,
        "com.amazonaws"          % "aws-java-sdk-dynamodb"   % awsSdkVersion % Test,
        "org.scala-lang.modules" %% "scala-java8-compat"     % "0.9.1"
      ),
    parallelExecution in Test := false
  )

val `akka-kinesis` = (project in file("."))
  .settings(baseSettings)
  .settings(name := "akka-kinesis")
  .aggregate(`akka-kinesis-kpl`, `akka-kinesis-kcl`)
