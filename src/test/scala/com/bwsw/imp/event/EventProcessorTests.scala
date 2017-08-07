package com.bwsw.imp.event

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.imp.activity.{Activity, ActivityMatcherRegistry, Environment}
import com.bwsw.imp.message.memory.MemoryMessageQueue
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 06.08.17.
  */
class EventProcessorTests extends FlatSpec with Matchers {
  it should "start/stop properly" in {
    val registry = new ActivityMatcherRegistry(new Environment)
    val eventQueue = new MemoryMessageQueue
    val activityQueue = new MemoryMessageQueue
    val eventProcessor = new EventProcessor(eventQueue = eventQueue, activityQueue = activityQueue,
      activityMatcherRegistry = registry, estimator = new PassThroughtEstimator)
    eventProcessor.start()
    eventProcessor.stop()
  }

  it should "get events from eventQueue and put actions to ActionQueue" in {
    val latch = new CountDownLatch(1)
    val registry = new ActivityMatcherRegistry(new Environment)
    val activity = new Activity {
      override def activate(e: Environment): Seq[Activity] = Nil
    }

    registry.register((environment: Environment, event: Event) => {
      latch.countDown()
      List(activity)
    })
    val eventQueue = new MemoryMessageQueue
    eventQueue.put(new Event)
    val activityQueue = new MemoryMessageQueue
    val eventProcessor = new EventProcessor(eventQueue = eventQueue, activityQueue = activityQueue,
      activityMatcherRegistry = registry, estimator = new PassThroughtEstimator)
    eventProcessor.start()
    latch.await(1, TimeUnit.SECONDS)
    eventProcessor.stop()
    activityQueue.get shouldBe Seq(activity)
  }
}
