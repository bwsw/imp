package com.bwsw.cloudstack.imp.activity

import com.bwsw.imp.activity.{Activity, ActivityMatcher, ActivityQueue}
import com.bwsw.imp.event.Event
import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by Ivan Kudryavtsev on 01.08.17.
  */

object ActivityMatcherGenerator {
  def getTrivialActionMatcher(): ActivityMatcher = {
    (event: Event) => {
      List(new Activity(new ActivityQueue) {
        override def run(): Unit = {}
      })
    }
  }
}

class ActivityMatcherTests extends FlatSpec with Matchers {
  it should "generate events properly" in {
    val f = ActivityMatcherGenerator.getTrivialActionMatcher

    val actionList = f.generate(new Event)
    actionList.isInstanceOf[List[Activity]] shouldBe true
    actionList.isEmpty shouldBe false
  }
}
