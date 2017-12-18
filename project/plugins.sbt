resolvers ++= Seq(
  Classpaths.typesafeReleases,
  Classpaths.typesafeSnapshots,
  "Sonatype OSS Snapshot Repository" at "https://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype OSS Release Repository" at "https://oss.sonatype.org/content/repositories/releases/",
  "Kamon Repository Snapshots"  at "http://snapshots.kamon.io"
)

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.0")

addSbtPlugin("com.lucidchart" % "sbt-scalafmt" % "1.14")

addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.3.2")

addSbtPlugin("com.mintbeans" % "sbt-ecr" % "0.8.0")