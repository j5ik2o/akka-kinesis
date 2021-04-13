import Dependencies._

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
  scalaVersion := Versions.scala212Version,
  crossScalaVersions := Seq(Versions.scala212Version, Versions.scala213Version),
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

val dependenciesCommonSettings = Seq(
  resolvers ++= Seq(
      "Sonatype OSS Snapshot Repository" at "https://oss.sonatype.org/content/repositories/snapshots/",
      "Sonatype OSS Release Repository" at "https://oss.sonatype.org/content/repositories/releases/",
      Resolver.bintrayRepo("hseeberger", "maven")
    ),
  libraryDependencies ++= Seq(
      typesafe.akka.slf4j,
      typesafe.akka.stream,
      amazonAws.kinesis,
      dimafeng.testcontainersScalatest  % Test,
      dimafeng.testcontainersLocalstack % Test,
      scalatest.scalatest               % Test,
      logback.classic                   % Test,
      typesafe.akka.testkit             % Test,
      typesafe.akka.streamTestkit       % Test
    ),
  Test / fork := true,
  envVars in Test := Map("AWS_CBOR_DISABLE" -> "1")
)

val `akka-kinesis-kpl` = (project in file("akka-kinesis-kpl"))
  .settings(baseSettings, dependenciesCommonSettings, deploySettings)
  .settings(
    name := "akka-kinesis-kpl",
    libraryDependencies ++= Seq(
        amazonAws.kinesisProducer,
        amazonAws.cloudwatch % Test,
        amazonAws.dynamodb   % Test
      ),
    parallelExecution in Test := false
  )

val `akka-kinesis-kcl` = (project in file("akka-kinesis-kcl"))
  .settings(baseSettings, dependenciesCommonSettings, deploySettings)
  .settings(
    name := "akka-kinesis-kcl",
    libraryDependencies ++= Seq(
        iheart.ficus,
        amazonAws.kinesisClient,
        scalaLang.scalaJava8Compat,
        amazonAws.streamKinesisAdaptor % Test,
        amazonAws.cloudwatch           % Test,
        amazonAws.dynamodb             % Test
      ),
    parallelExecution in Test := false
  )

val `akka-kinesis-kcl-dynamodb-streams` = (project in file("akka-kinesis-kcl-dynamodb-streams"))
  .settings(baseSettings, dependenciesCommonSettings, deploySettings)
  .settings(
    name := "akka-kinesis-kcl",
    libraryDependencies ++= Seq(
        amazonAws.dynamodb,
        amazonAws.streamKinesisAdaptor,
        amazonAws.cloudwatch % Test,
        amazonAws.dynamodb   % Test
      ),
    parallelExecution in Test := false
  ).dependsOn(`akka-kinesis-kcl` % "compile->compile;test->test")

val `akka-kinesis-root` = (project in file("."))
  .settings(baseSettings, deploySettings)
  .settings(name := "akka-kinesis-root")
  .aggregate(`akka-kinesis-kpl`, `akka-kinesis-kcl`, `akka-kinesis-kcl-dynamodb-streams`)
