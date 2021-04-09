val scala212Version = "2.12.13"
val scala213Version = "2.13.5"

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
  crossScalaVersions := Seq(scala212Version, scala213Version),
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

val awsSdkVersion              = "1.11.996"
val akkaVersion                = "2.6.13"
val testcontainersScalaVersion = "0.39.3"
val scalaTestVersion           = "3.2.6"
val logbackVersion             = "1.2.3"

val dependenciesCommonSettings = Seq(
  resolvers ++= Seq(
      "Sonatype OSS Snapshot Repository" at "https://oss.sonatype.org/content/repositories/snapshots/",
      "Sonatype OSS Release Repository" at "https://oss.sonatype.org/content/repositories/releases/",
      Resolver.bintrayRepo("hseeberger", "maven")
    ),
  libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-slf4j"                      % akkaVersion,
      "com.typesafe.akka" %% "akka-stream"                     % akkaVersion,
      "com.amazonaws"     % "aws-java-sdk-kinesis"             % awsSdkVersion,
      "com.dimafeng"      %% "testcontainers-scala-scalatest"  % testcontainersScalaVersion % Test,
      "com.dimafeng"      %% "testcontainers-scala-localstack" % testcontainersScalaVersion % Test,
      "org.scalatest"     %% "scalatest"                       % scalaTestVersion % Test,
      "ch.qos.logback"    % "logback-classic"                  % logbackVersion % Test,
      "com.typesafe.akka" %% "akka-testkit"                    % akkaVersion % Test,
      "com.typesafe.akka" %% "akka-stream-testkit"             % akkaVersion % Test
    ),
  Test / fork := true,
  envVars in Test := Map("AWS_CBOR_DISABLE" -> "1")
)

val `akka-kinesis-kpl` = (project in file("akka-kinesis-kpl"))
  .settings(baseSettings, dependenciesCommonSettings, deploySettings)
  .settings(
    name := "akka-kinesis-kpl",
    libraryDependencies ++= Seq(
        "com.amazonaws" % "amazon-kinesis-producer" % "0.14.6",
        "com.amazonaws" % "aws-java-sdk-cloudwatch" % awsSdkVersion % Test,
        "com.amazonaws" % "aws-java-sdk-dynamodb"   % awsSdkVersion % Test
      ),
    parallelExecution in Test := false
  )

val `akka-kinesis-kcl` = (project in file("akka-kinesis-kcl"))
  .settings(baseSettings, dependenciesCommonSettings, deploySettings)
  .settings(
    name := "akka-kinesis-kcl",
    libraryDependencies ++= Seq(
        "com.iheart"             %% "ficus"                           % "1.5.0",
        "com.amazonaws"          % "amazon-kinesis-client"            % "1.14.2",
        "org.scala-lang.modules" %% "scala-java8-compat"              % "0.9.1",
        "com.amazonaws"          % "dynamodb-streams-kinesis-adapter" % "1.5.1" % Test,
        "com.amazonaws"          % "aws-java-sdk-cloudwatch"          % awsSdkVersion % Test,
        "com.amazonaws"          % "aws-java-sdk-dynamodb"            % awsSdkVersion % Test
      ),
    parallelExecution in Test := false
  )

val `akka-kinesis-kcl-dynamodb-streams` = (project in file("akka-kinesis-kcl-dynamodb-streams"))
  .settings(baseSettings, dependenciesCommonSettings, deploySettings)
  .settings(
    name := "akka-kinesis-kcl",
    libraryDependencies ++= Seq(
        "com.amazonaws" % "aws-java-sdk-dynamodb"            % awsSdkVersion,
        "com.amazonaws" % "dynamodb-streams-kinesis-adapter" % "1.5.1",
        "com.amazonaws" % "aws-java-sdk-cloudwatch"          % awsSdkVersion % Test,
        "com.amazonaws" % "aws-java-sdk-dynamodb"            % awsSdkVersion % Test
      ),
    parallelExecution in Test := false
  ).dependsOn(`akka-kinesis-kcl` % "compile->compile;test->test")

val `akka-kinesis-root` = (project in file("."))
  .settings(baseSettings, deploySettings)
  .settings(name := "akka-kinesis-root")
  .aggregate(`akka-kinesis-kpl`, `akka-kinesis-kcl`, `akka-kinesis-kcl-dynamodb-streams`)
