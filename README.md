# akka-kinesis

[![Scala CI](https://github.com/j5ik2o/akka-kinesis/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/j5ik2o/akka-kinesis/actions/workflows/ci.yml)
[![Scala Steward badge](https://img.shields.io/badge/Scala_Steward-helping-blue.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://scala-steward.org)
[![Mergify Status](https://img.shields.io/endpoint.svg?url=https://gh.mergify.io/badges/j5ik2o/akka-kinesis&style=flat)](https://mergify.io)
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
