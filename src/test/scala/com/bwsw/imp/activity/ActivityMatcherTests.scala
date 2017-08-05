package com.bwsw.imp.activity

import com.bwsw.imp.event.Event
import com.bwsw.imp.message.{DelayedMessage, Message, MessageQueue}
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 01.08.17.
  */

object ActivityMatcherGenerator {
  def getTrivialActionMatcher: ActivityMatcher = {
    (event: Event) => {
      List(new Activity(new MessageQueue {
        override def get: Option[Message] = ???
        override def put(message: Message, delay: Long): Unit = ???
        override def put(message: DelayedMessage): Unit = ???
      }) {
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
