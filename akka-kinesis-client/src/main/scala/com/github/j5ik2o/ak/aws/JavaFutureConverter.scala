package com.github.j5ik2o.ak.aws

import java.util.concurrent.{ CompletableFuture, ExecutionException, Future => JavaFuture }

import scala.concurrent.{ ExecutionContext, Future => ScalaFuture }
import scala.util.{ Failure, Success }

object JavaFutureConverter {

  implicit class JavaFutureExtension[A](val self: JavaFuture[A]) extends AnyVal {
    def toScala(implicit ec: ExecutionContext): ScalaFuture[A] = JavaFutureConverter.toScala(self)
  }

  implicit class ScalaFutureExtension[A](val self: ScalaFuture[A]) extends AnyVal {
    def toJava(implicit ec: ExecutionContext): JavaFuture[A] = JavaFutureConverter.toJava(self)
  }

  def toScala[A](jf: JavaFuture[A])(implicit ec: ExecutionContext): ScalaFuture[A] = {
    ScalaFuture(jf.get()).transform {
      case Failure(e: ExecutionException) =>
        Failure(e.getCause)
      case x => x
    }
  }

  def toJava[A](sf: ScalaFuture[A])(implicit ec: ExecutionContext): JavaFuture[A] = {
    val cs = new CompletableFuture[A]()
    sf.onComplete {
      case Success(result) =>
        cs.complete(result)
      case Failure(ex) =>
        cs.completeExceptionally(ex)
    }
    cs
  }

}
