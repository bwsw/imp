package com.bwsw.imp.message

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 04.08.17.
  */
class BypassMessageFilterTests extends FlatSpec with Matchers {
  it should "always return true" in {
    val throttler = new BypassMessageFilter
    throttler.filterPut(new Message {}) shouldBe true
    throttler.filterGet(new Message {}) shouldBe true
  }

}
