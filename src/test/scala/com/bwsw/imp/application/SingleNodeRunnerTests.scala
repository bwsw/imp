package com.bwsw.imp.application

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by ivan on 09.08.17.
  */
class SingleNodeRunnerTests extends FlatSpec with Matchers {
  it should "run and stop properly" in {
    val runner = new SingleNodeRunner
    runner.start()
    runner.stop()
  }
}
