package com.bwsw.cloudstack.imp.event

import com.bwsw.imp.event.Event
import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by Ivan Kudryavtsev on 01.08.17.
  */
class EventTests extends FlatSpec with Matchers {
  val KEY = "key"
  val VALUE = "value"
  it should "create event and handle operations" in {
    val e = new Event
    e.setProperty(KEY, VALUE)
    e.getProperty(KEY) shouldBe VALUE
  }

}
