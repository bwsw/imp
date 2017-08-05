package com.bwsw.imp.activity

import com.bwsw.imp.event.Event
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 01.08.17.
  */

object ActivityMatcherGenerator {
  def getTrivialActionMatcher: ActivityMatcher = {
    (e: Environment, event: Event) => {
      List(new Activity {
        override def activate(e: Environment): Seq[Activity] = ???
      })
    }
  }
}

class ActivityMatcherTests extends FlatSpec with Matchers {
  it should "generate events properly" in {
    val f = ActivityMatcherGenerator.getTrivialActionMatcher

    val actionList = f.spawn(new Environment, new Event)
    actionList.isInstanceOf[List[Activity]] shouldBe true
    actionList.isEmpty shouldBe false
  }
}
