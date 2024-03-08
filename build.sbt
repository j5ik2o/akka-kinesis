import Dependencies._

def crossScalacOptions(scalaVersion: String): Seq[String] =
  CrossVersion.partialVersion(scalaVersion) match {
    case Some((3L, _)) =>
      Seq(
        "-source:3.0-migration",
        "-Xignore-scala2-macros"
      )
    case Some((2L, scalaMajor)) if scalaMajor >= 12 =>
      Seq(
        "-Ydelambdafy:method",
        "-target:jvm-1.8",
        "-Yrangepos",
        "-Ywarn-unused"
      )
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
  crossScalaVersions := Seq(Versions.scala213Version, Versions.scala3Version),
  scalacOptions ++= (Seq(
    "-unchecked",
    "-feature",
    "-deprecation",
    "-encoding",
    "UTF-8",
    "-language:_"
  ) ++ crossScalacOptions(scalaVersion.value)),
  resolvers ++= Resolver.sonatypeOssRepos("snapshots") ++ Seq(
    "Seasar Repository" at "https://maven.seasar.org/maven2/"
  ),
  Test / publishArtifact := false,
  Test / parallelExecution := false,
  Compile / doc / sources := {
    val old = (Compile / doc / sources).value
    if (scalaVersion.value == Versions.scala3Version) {
      Nil
    } else {
      old
    }
  },
  semanticdbEnabled := true,
  semanticdbVersion := scalafixSemanticdb.revision,
  ThisBuild / scalafixScalaBinaryVersion := (CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, _)) => CrossVersion.binaryScalaVersion(scalaVersion.value)
    case _            => CrossVersion.binaryScalaVersion(Versions.scala212Version)
  }),
  envVars := Map(
    "AWS_REGION" -> "ap-northeast-1"
  )
)

val dependenciesCommonSettings = Seq(
  libraryDependencies ++= Seq(
    amazonAws.kinesis,
    slf4j.api,
    logback.classic     % Test,
    scalatest.scalatest % Test
  ),
  libraryDependencies ++= Seq(
    typesafe.akka.actor excludeAll (ExclusionRule(organization = "org.scala-lang.modules")),
    typesafe.akka.slf4j excludeAll (ExclusionRule(organization = "org.slf4j")),
    typesafe.akka.stream excludeAll (ExclusionRule(organization = "org.slf4j")),
    dimafeng.testcontainersScalatest  % Test excludeAll (ExclusionRule(organization = "org.slf4j")),
    dimafeng.testcontainersLocalstack % Test excludeAll (ExclusionRule(organization = "org.slf4j")),
    typesafe.akka.testkit             % Test excludeAll (ExclusionRule(organization = "org.slf4j")),
    typesafe.akka.streamTestkit       % Test excludeAll (ExclusionRule(organization = "org.slf4j"))
  ).map(_.cross(CrossVersion.for3Use2_13)),
  Test / fork := true,
  Test / envVars := Map("AWS_CBOR_DISABLE" -> "1")
)

val `akka-kinesis-kpl` = (project in file("akka-kinesis-kpl"))
  .settings(baseSettings, dependenciesCommonSettings)
  .settings(
    name := "akka-kinesis-kpl",
    libraryDependencies ++= Seq(
      amazonAws.kinesisProducer excludeAll (ExclusionRule(organization = "org.slf4j")),
      amazonAws.cloudwatch % Test excludeAll (ExclusionRule(organization = "org.slf4j")),
      amazonAws.dynamodb   % Test excludeAll (ExclusionRule(organization = "org.slf4j"))
    ),
    Test / parallelExecution := false
  )

val `akka-kinesis-kcl` = (project in file("akka-kinesis-kcl"))
  .settings(baseSettings, dependenciesCommonSettings)
  .settings(
    name := "akka-kinesis-kcl",
    libraryDependencies ++= Seq(
      amazonAws.kinesisClient,
      amazonAws.streamKinesisAdaptor % Test,
      amazonAws.cloudwatch           % Test,
      amazonAws.dynamodb             % Test
    ),
    libraryDependencies ++= Seq(
      iheart.ficus,
      scalaLang.scalaJava8Compat
    ).map(_.cross(CrossVersion.for3Use2_13)),
    Test / parallelExecution := false
  )

val `akka-kinesis-kcl-dynamodb-streams` = (project in file("akka-kinesis-kcl-dynamodb-streams"))
  .settings(baseSettings, dependenciesCommonSettings)
  .settings(
    name := "akka-kinesis-kcl-dynamodb-streams",
    libraryDependencies ++= Seq(
      amazonAws.kinesisClient,
      amazonAws.dynamodb,
      amazonAws.streamKinesisAdaptor,
      amazonAws.cloudwatch % Test,
      amazonAws.dynamodb   % Test
    ),
    libraryDependencies ++= Seq(
      iheart.ficus,
      scalaLang.scalaJava8Compat
    ).map(_.cross(CrossVersion.for3Use2_13)),
    Test / parallelExecution := false
  ).dependsOn(`akka-kinesis-kcl` % "compile->compile;test->test")

val `akka-kinesis-root` = (project in file("."))
  .settings(baseSettings)
  .settings(name := "akka-kinesis-root")
  .aggregate(`akka-kinesis-kpl`, `akka-kinesis-kcl`, `akka-kinesis-kcl-dynamodb-streams`)

// --- Custom commands
addCommandAlias("lint", ";scalafmtCheck;test:scalafmtCheck;scalafmtSbtCheck;scalafixAll --check")
addCommandAlias("fmt", ";scalafmtAll;scalafmtSbt;scalafix RemoveUnused")
