package com.bwsw.imp.common

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 07.08.17.
  */
class StartStopBehaviourTests extends FlatSpec with Matchers {
  it should "start properly" in {
    new StartStopBehaviour {}.start()
  }

  it should "stop properly" in {
    val b = new StartStopBehaviour {}
    b.start()
    b.stop()
  }

  it should "not permit double start" in {
    val b = new StartStopBehaviour {}

    b.start()

    intercept[IllegalStateException] {
      b.start()
    }
  }

  it should "not permit double stop" in {
    val b = new StartStopBehaviour {}
    b.start()
    b.stop()
    intercept[IllegalStateException] {
      b.stop()
    }
  }
}
