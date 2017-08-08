package com.bwsw.imp.activity

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.imp.message.memory.MemoryMessageQueue
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 08.08.17.
  */
class ActivityRunnerTests extends FlatSpec with Matchers {
  it should "start and stop properly" in {
    val runner = new ActivityRunner(regularActivityQueue = new MemoryMessageQueue,
      delayedActivityQueue = new MemoryMessageQueue, environment = new Environment)
    runner.start()
    runner.stop()
  }

  it should "run single regular activity properly" in {
    val regularActivityQueue = new MemoryMessageQueue
    val latch = new CountDownLatch(1)
    val runner = new ActivityRunner(regularActivityQueue = regularActivityQueue,
      delayedActivityQueue = new MemoryMessageQueue, environment = new Environment)
    runner.start()
    regularActivityQueue.put(new Activity {
      override def activate(e: Environment): Seq[Activity] = {
        latch.countDown()
        Nil
      }
    })
    latch.await(10, TimeUnit.SECONDS) shouldBe true
    runner.stop()
  }

  it should "run single regular activity which returns self for next run properly" in {
    val regularActivityQueue = new MemoryMessageQueue
    val latch = new CountDownLatch(2)
    val runner = new ActivityRunner(regularActivityQueue = regularActivityQueue,
      delayedActivityQueue = new MemoryMessageQueue, environment = new Environment)
    runner.start()
    regularActivityQueue.put(new Activity {
      override def activate(e: Environment): Seq[Activity] = {
        latch.countDown()
        if(latch.getCount > 0)
          Seq(this)
        else
          Nil
      }
    })
    latch.await(10, TimeUnit.SECONDS) shouldBe true
    runner.stop()
  }

  it should "run single regular activity which returns next for next run properly" in {
    val regularActivityQueue = new MemoryMessageQueue
    val latch = new CountDownLatch(2)
    val runner = new ActivityRunner(regularActivityQueue = regularActivityQueue,
      delayedActivityQueue = new MemoryMessageQueue, environment = new Environment)
    runner.start()
    regularActivityQueue.put(new Activity {
      override def activate(e: Environment): Seq[Activity] = {
        latch.countDown()
        Seq(new Activity {
          override def activate(e: Environment): Seq[Activity] = {
            latch.countDown()
            Nil
          }
        })
      }
    })
    latch.await(10, TimeUnit.SECONDS) shouldBe true
    runner.stop()
  }
}
