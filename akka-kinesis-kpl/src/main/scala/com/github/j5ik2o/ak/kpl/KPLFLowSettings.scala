package com.github.j5ik2o.ak.kpl

import scala.concurrent.duration.FiniteDuration

case class KPLFLowSettings(maxRetries: Int, backoffStrategy: RetryBackoffStrategy, retryInitialTimeout: FiniteDuration)
