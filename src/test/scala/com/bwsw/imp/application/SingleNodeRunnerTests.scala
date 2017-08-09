package com.bwsw.imp.application

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.imp.activity.{Activity, Environment}
import com.bwsw.imp.event.Event
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by ivan on 09.08.17.
  */
class SingleNodeRunnerTests extends FlatSpec with Matchers {
  it should "run and stop properly" in {
    val runner = new SingleNodeRunner
    runner.start()
    runner.stop()
  }

  it should "get message and transform it to activity" in {
    val latch = new CountDownLatch(1)
    val runner = new SingleNodeRunner
    runner.start()
    runner.registerActivityMatcher((environment: Environment, event: Event) => List(new Activity {
      override def activate(e: Environment): Seq[Activity] = {
        latch.countDown()
        Nil
      }
    }))
    runner.getEventQueue().put(new Event {})
    latch.await(10, TimeUnit.SECONDS) shouldBe true
    runner.stop()
  }
}
