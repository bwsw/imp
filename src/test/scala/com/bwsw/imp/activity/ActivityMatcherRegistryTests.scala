package com.bwsw.imp.activity

import com.bwsw.imp.event.Event
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 01.08.17.
  */
class ActivityMatcherRegistryTests extends FlatSpec with Matchers {
  it should "gather events from different factories" in {
    val f1 = ActivityMatcherGenerator.getTrivialActionMatcher
    val f2 = ActivityMatcherGenerator.getTrivialActionMatcher

    val registry = new ActivityMatcherRegistry
    registry
      .register(f1)
      .register(f2)

    val actionList = registry.generate(new Event)

    actionList.isInstanceOf[List[Activity]] shouldBe true
    actionList.size shouldBe 2

  }
}
