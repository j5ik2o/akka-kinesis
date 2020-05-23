val commonSettings = Seq(
  version := "1.0.0-SNAPSHOT",
  scalaVersion := "2.12.4",
  scalafmtOnCompile in ThisBuild := true,
  scalafmtTestOnCompile in ThisBuild := true
)

val awsSdkVersion = "1.11.788"
val akkaVersion   = "2.5.31"
val circeVersion  = "0.12.3"

val dependenciesCommonSettings = Seq(
  resolvers ++= Seq(
    "Sonatype OSS Snapshot Repository" at "https://oss.sonatype.org/content/repositories/snapshots/",
    "Sonatype OSS Release Repository" at "https://oss.sonatype.org/content/repositories/releases/",
    Resolver.bintrayRepo("hseeberger", "maven")
  ),
  libraryDependencies ++= Seq(
    "com.beachape"      %% "enumeratum"          % "1.6.1",
    "org.scalactic"     %% "scalactic"           % "3.1.2",
    "org.scalatest"     %% "scalatest"           % "3.1.2" % Test,
    "org.scalacheck"    %% "scalacheck"          % "1.14.3" % Test,
    "org.sisioh"        %% "sisioh-config"       % "0.0.15",
    "com.typesafe"      % "config"               % "1.4.0",
    "ch.qos.logback"    % "logback-classic"      % "1.2.3",
    "com.typesafe.akka" %% "akka-slf4j"          % akkaVersion,
    "com.typesafe.akka" %% "akka-actor"          % akkaVersion,
    "com.typesafe.akka" %% "akka-testkit"        % akkaVersion % Test,
    "com.typesafe.akka" %% "akka-stream"         % akkaVersion,
    "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test,
    "com.amazonaws"     % "aws-java-sdk-core"    % awsSdkVersion,
    "com.amazonaws"     % "aws-java-sdk-kinesis" % awsSdkVersion
  ),
  fork in Test := true,
  envVars in Test := Map("AWS_CBOR_DISABLE" -> "1")
)

val `akka-kinesis-client` = (project in file("akka-kinesis-client"))
  .settings(commonSettings, dependenciesCommonSettings)
  .settings(
    name := "akka-kinesis-client",
    libraryDependencies ++= Seq(
      )
  )

val `akka-kinesis-kpl` = (project in file("akka-kinesis-kpl"))
  .settings(commonSettings, dependenciesCommonSettings)
  .settings(
    name := "akka-kinesis-kpl",
    libraryDependencies ++= Seq(
      "com.amazonaws" % "amazon-kinesis-producer" % "0.14.0"
    ),
    parallelExecution in Test := false
  )
  .dependsOn(`akka-kinesis-client`)

val `akka-kinesis-kcl` = (project in file("akka-kinesis-kcl"))
  .settings(commonSettings, dependenciesCommonSettings)
  .settings(
    name := "akka-kinesis-kcl",
    libraryDependencies ++= Seq(
      "com.amazonaws" % "amazon-kinesis-client"   % "1.11.2",
      "com.amazonaws" % "aws-java-sdk-cloudwatch" % awsSdkVersion,
      "com.amazonaws" % "aws-java-sdk-dynamodb"   % awsSdkVersion
    ),
    parallelExecution in Test := false
  )
  .dependsOn(`akka-kinesis-client`)

val `akka-kinesis-persistence` = (project in file("akka-kinesis-persistence"))
  .settings(commonSettings, dependenciesCommonSettings)
  .settings(
    name := "akka-kinesis-persistence",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-persistence"       % akkaVersion,
      "com.typesafe.akka" %% "akka-persistence-query" % akkaVersion,
      "com.typesafe.akka" %% "akka-persistence-tck"   % akkaVersion % Test,
      "io.circe"          %% "circe-core"             % circeVersion,
      "io.circe"          %% "circe-generic"          % circeVersion,
      "io.circe"          %% "circe-parser"           % circeVersion
    ),
    parallelExecution in Test := false
  )
  .dependsOn(`akka-kinesis-kcl`, `akka-kinesis-kpl`)

val `akka-kinesis` = (project in file("."))
  .settings(commonSettings)
  .settings(name := "akka-kinesis")
  .aggregate(`akka-kinesis-client`, `akka-kinesis-kpl`, `akka-kinesis-kcl`, `akka-kinesis-persistence`)
