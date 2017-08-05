package com.bwsw.imp.message

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 05.08.17.
  */
class DeduplicatingMessageFilterTests extends FlatSpec with Matchers {
  it should "filter previously added messages and pass new ones" in {
    val mf = new DeduplicatingMessageFilter(scala.collection.mutable.Map[String, Boolean]().empty)
    val message1 = new Message {}
    mf.filterPut(message1) shouldBe true
    mf.filterPut(message1) shouldBe false

    val message2 = new Message {}
    mf.filterPut(message2) shouldBe true
  }
}
