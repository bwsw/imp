package com.bwsw.imp.message

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 04.08.17.
  */
class BypassThrottlerTests extends FlatSpec with Matchers {
  it should "always return true" in {
    val throttler = new BypassThrottler
    throttler.passEgress(new Message {}) shouldBe true
    throttler.passIngress(new Message {}) shouldBe true
  }

}
