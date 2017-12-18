package com.github.j5ik2o.ak

import java.util.concurrent.{ ExecutionException, Future => JavaFuture }

import scala.concurrent.{ ExecutionContext, Future => ScalaFuture }
import scala.util.Failure

object JavaFutureConverter {

  implicit class FutureExtension[A](val self: JavaFuture[A]) extends AnyVal {
    def toScala(implicit ec: ExecutionContext): ScalaFuture[A] = JavaFutureConverter.toScala(self)
  }

  def toScala[A](jf: JavaFuture[A])(implicit ec: ExecutionContext): ScalaFuture[A] = {
    ScalaFuture(jf.get()).transform {
      case Failure(e: ExecutionException) =>
        Failure(e.getCause)
      case x => x
    }
  }

}
