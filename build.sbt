import Dependencies._

def crossScalacOptions(scalaVersion: String): Seq[String] = CrossVersion.partialVersion(scalaVersion) match {
  case Some((3L, scalaMajor)) =>
    Seq(
      "-unchecked",
      "-source:3.0-migration"
    )
  case Some((2L, scalaMajor)) if scalaMajor >= 12 =>
    Seq(
      "-Ydelambdafy:method",
      "-target:jvm-1.8",
      "-Yrangepos",
      "-Ywarn-unused"
    )
  case Some((2L, scalaMajor)) if scalaMajor <= 11 =>
    Seq("-Yinline-warnings")
}

lazy val baseSettings = Seq(
  organization := "com.github.j5ik2o",
  homepage := Some(url("https://github.com/j5ik2o/akka-kinesis")),
  licenses := List("The MIT License" -> url("http://opensource.org/licenses/MIT")),
  developers := List(
      Developer(
        id = "j5ik2o",
        name = "Junichi Kato",
        email = "j5ik2o@gmail.com",
        url = url("https://blog.j5ik2o.me")
      )
    ),
  scalaVersion := Versions.scala213Version,
  crossScalaVersions := Seq(Versions.scala212Version, Versions.scala213Version, Versions.scala3Version),
  scalacOptions ++= (Seq(
      "-feature",
      "-deprecation",
      "-unchecked",
      "-encoding",
      "UTF-8",
      "-language:_"
    ) ++ crossScalacOptions(scalaVersion.value)),
  resolvers ++= Seq(
      Resolver.sonatypeRepo("snapshots"),
      Resolver.sonatypeRepo("releases"),
      "Seasar Repository" at "https://maven.seasar.org/maven2/",
      "DynamoDB Local Repository" at "https://s3-us-west-2.amazonaws.com/dynamodb-local/release"
    ),
  libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % Test,
  ThisBuild / scalafixScalaBinaryVersion := CrossVersion.binaryScalaVersion(scalaVersion.value),
  semanticdbEnabled := true,
  semanticdbVersion := scalafixSemanticdb.revision,
  Test / publishArtifact := false,
  Test / parallelExecution := false,
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
  libraryDependencies ++= (
      Seq(
        typesafe.akka.slf4j,
        typesafe.akka.stream,
        scalatest.scalatest               % Test,
        dimafeng.testcontainersScalatest  % Test,
        dimafeng.testcontainersLocalstack % Test,
        typesafe.akka.testkit             % Test,
        typesafe.akka.streamTestkit       % Test
      )
    ).map(_.cross(CrossVersion.for3Use2_13)),
  libraryDependencies ++= Seq(
      amazonAws.kinesis,
      logback.classic % Test
    ),
  Test / fork := true,
  Test / envVars := Map("AWS_CBOR_DISABLE" -> "1")
)

val `akka-kinesis-kpl` = (project in file("akka-kinesis-kpl"))
  .settings(baseSettings, dependenciesCommonSettings)
  .settings(
    name := "akka-kinesis-kpl",
    libraryDependencies ++= Seq(
        amazonAws.kinesisProducer,
        amazonAws.cloudwatch % Test,
        amazonAws.dynamodb   % Test
      ),
    Test / parallelExecution := false
  )

val `akka-kinesis-kcl` = (project in file("akka-kinesis-kcl"))
  .settings(baseSettings, dependenciesCommonSettings)
  .settings(
    name := "akka-kinesis-kcl",
    libraryDependencies ++= Seq(
        iheart.ficus,
        scalaLang.scalaJava8Compat
      ).map(_.cross(CrossVersion.for3Use2_13)),
    libraryDependencies ++= Seq(
        amazonAws.kinesisClient,
        amazonAws.streamKinesisAdaptor % Test,
        amazonAws.cloudwatch           % Test,
        amazonAws.dynamodb             % Test
      ),
    Test / parallelExecution := false
  )

val `akka-kinesis-kcl-dynamodb-streams` = (project in file("akka-kinesis-kcl-dynamodb-streams"))
  .settings(baseSettings, dependenciesCommonSettings)
  .settings(
    name := "akka-kinesis-kcl",
    libraryDependencies ++= Seq(
        iheart.ficus,
        scalaLang.scalaJava8Compat
      ).map(_.cross(CrossVersion.for3Use2_13)),
    libraryDependencies ++= Seq(
        amazonAws.kinesisClient,
        amazonAws.dynamodb,
        amazonAws.streamKinesisAdaptor,
        amazonAws.cloudwatch % Test,
        amazonAws.dynamodb   % Test
      ),
    Test / parallelExecution := false
  ).dependsOn(`akka-kinesis-kcl` % "compile->compile;test->test")

val `akka-kinesis-root` = (project in file("."))
  .settings(baseSettings)
  .settings(name := "akka-kinesis-root")
  .aggregate(`akka-kinesis-kpl`, `akka-kinesis-kcl`, `akka-kinesis-kcl-dynamodb-streams`)

// --- Custom commands
addCommandAlias("lint", ";scalafmtCheck;test:scalafmtCheck;scalafmtSbtCheck;scalafixAll --check")
addCommandAlias("fmt", ";scalafmtAll;scalafmtSbt")
