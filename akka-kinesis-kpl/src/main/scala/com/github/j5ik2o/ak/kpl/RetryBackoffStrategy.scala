package com.github.j5ik2o.ak.kpl

import enumeratum.{ Enum, EnumEntry }

sealed trait RetryBackoffStrategy extends EnumEntry

object RetryBackoffStrategy extends Enum[RetryBackoffStrategy] {

  override def values = findValues

  case object Exponential extends RetryBackoffStrategy

  case object Lineal extends RetryBackoffStrategy

}
