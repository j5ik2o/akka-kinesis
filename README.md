# akka-kinesis

[![CircleCI](https://circleci.com/gh/j5ik2o/akka-kinesis/tree/master.svg?style=shield)](https://circleci.com/gh/j5ik2o/akka-kinesis/tree/master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.j5ik2o/akka-kinesis-kcl_2.13/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.j5ik2o/akka-kinesis-kcl_2.13)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

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
