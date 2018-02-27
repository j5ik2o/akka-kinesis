# akka-kinesis

akka-kinesis supports Akka commponets for AWS Kinesis.

## Support features

- KPLFlow
- KCLSource
- KinesisAsyncWriteJournal
- KinesisScalaReadJournal

## How to tests

```sh
$ AWS_PROFILE=akka-kinesis KPL_STREAM_NAME=akka-kinesis sbt 'akka-kinesis-persistence/testOnly com.github.j5ik2o.ak.persistence.KinesisAsyncWriteJournalSpec'
```

```sh
$ AWS_PROFILE=akka-kinesis KPL_STREAM_NAME=akka-kinesis sbt 'akka-kinesis-persistence/testOnly com.github.j5ik2o.ak.persistence.KinesisScalaReadJournalSpec'
```
