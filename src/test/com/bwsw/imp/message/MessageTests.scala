package com.bwsw.imp.message

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 01.08.17.
  */
class MessageTests extends FlatSpec with Matchers {
  val KEY = "key"
  val VALUE = "value"
  it should "create event and handle operations" in {
    val e = new Message {}
    e.setProperty(KEY, VALUE)
    e.getProperty(KEY) shouldBe Some(VALUE)
  }

}
