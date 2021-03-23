# akka-kinesis

[![CircleCI](https://circleci.com/gh/j5ik2o/akka-kinesis/tree/master.svg?style=shield&circle-token=9f6f53d09f0fb87ee8ea81246e69683d668291cd)](https://circleci.com/gh/j5ik2o/akka-kinesis/tree/master)

akka-kinesis supports Akka commponets for AWS Kinesis.

## Support features

- KPLFlow
- KCLSource

## Installation

Add the following to your sbt build (2.12.x, 2.13.x):

```scala
// if snapshot
resolvers += "Sonatype OSS Release Repository" at "https://oss.sonatype.org/content/repositories/releases/"

val version = "..."

libraryDependencies += Seq(
  "com.github.j5ik2o" %% "akka-kinesis-kcl" % version,
  "com.github.j5ik2o" %% "akka-kinesis-kpl" % version
)
```
