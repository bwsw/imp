package com.bwsw.imp.event

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.imp.activity._
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
      activityMatcherRegistry = registry, estimator = new PassThroughActivityEstimator)
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
      activityMatcherRegistry = registry, estimator = new PassThroughActivityEstimator)
    eventProcessor.start()
    latch.await(1, TimeUnit.SECONDS) shouldBe true
    eventProcessor.stop()
    activityQueue.get shouldBe Seq(activity)
  }

  it should "get events from eventQueue and put actions to ActionQueue when first factory crashes" in {
    val latch = new CountDownLatch(1)
    val registry = new ActivityMatcherRegistry(new Environment)
    val activity = new Activity {
      override def activate(e: Environment): Seq[Activity] = Nil
    }

    registry.register((environment: Environment, event: Event) => {
      throw new IllegalArgumentException("fail")
    })

    registry.register((environment: Environment, event: Event) => {
      latch.countDown()
      List(activity)
    })

    val eventQueue = new MemoryMessageQueue
    eventQueue.put(new Event)
    val activityQueue = new MemoryMessageQueue
    val eventProcessor = new EventProcessor(eventQueue = eventQueue, activityQueue = activityQueue,
      activityMatcherRegistry = registry, estimator = new PassThroughActivityEstimator)
    eventProcessor.start()
    latch.await(1, TimeUnit.SECONDS) shouldBe true
    eventProcessor.stop()
    activityQueue.get shouldBe Seq(activity)
  }

  it should "get events and put delayed activity properly" in {
    val latch = new CountDownLatch(1)
    val registry = new ActivityMatcherRegistry(new Environment)
    val sleep = 100
    val activity = new DelayedActivity {
      override def activate(e: Environment): Seq[Activity] = Nil
      override def delay: Long = System.currentTimeMillis() + sleep
    }

    registry.register((environment: Environment, event: Event) => {
      latch.countDown()
      List(activity)
    })

    val eventQueue = new MemoryMessageQueue
    eventQueue.put(new Event)

    val activityQueue = new MemoryMessageQueue

    val eventProcessor = new EventProcessor(eventQueue = eventQueue, activityQueue = activityQueue,
      activityMatcherRegistry = registry, estimator = new PassThroughActivityEstimator)
    eventProcessor.start()
    latch.await(1, TimeUnit.SECONDS) shouldBe true
    eventProcessor.stop()
    Thread.sleep(sleep + 1)
    activityQueue.get shouldBe Seq(activity)
  }
}
