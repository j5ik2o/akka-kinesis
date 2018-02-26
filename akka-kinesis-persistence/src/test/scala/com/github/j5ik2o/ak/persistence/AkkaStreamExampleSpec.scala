package com.github.j5ik2o.ak.persistence

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.testkit.TestKit
import org.reactivestreams.{ Publisher, Subscriber }
import org.scalatest.FreeSpecLike

import scala.util.Random

class AkkaStreamExampleSpec extends TestKit(ActorSystem("AkkaStreamExampleSpec")) with FreeSpecLike {
  implicit val mat = ActorMaterializer()
  "stream" - {
    "iterator" in {}
  }
}
