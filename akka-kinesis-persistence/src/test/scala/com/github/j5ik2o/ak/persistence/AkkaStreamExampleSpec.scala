package com.github.j5ik2o.ak.persistence
import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Keep, Sink, Source }
import akka.testkit.TestKit
import org.scalatest.FreeSpecLike
import org.scalatest.concurrent.ScalaFutures

import scala.util.Random

class AkkaStreamExampleSpec extends TestKit(ActorSystem("AkkaStreamExampleSpec")) with FreeSpecLike with ScalaFutures {
  implicit val mat = ActorMaterializer()
  "stream" - {
    "iterator" in {
      val result = Source
        .single(Seq(1, 2, 3))
        .mapConcat { e =>
          e.toVector
        }
        .runWith(Sink.seq)
      println(result.futureValue)
//      val r = Source
//        .repeat(0)
//        .flatMapConcat { _ =>
//          Source.tick[Int](0 seconds, 3 seconds, Random.nextInt()).take(1)
//        }
//        .toMat(Sink.foreach(println))(Keep.left)
//        .run()

//      Thread.sleep(30 * 1000)
//      r
//
//      Source
//        .tick[Int](0 seconds, 3 seconds, Random.nextInt())
//        .take(1)
//        .toMat(Sink.foreach(println))(Keep.left)
//        .run()
//
//      Thread.sleep(30 * 1000)
    }
  }
}
