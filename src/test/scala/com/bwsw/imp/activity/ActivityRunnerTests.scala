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

  it should "run single regular activity which returns next one and it run properly" in {
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

  it should "run single regular activity which returns delayed one and it run properly as well" in {
    val regularActivityQueue = new MemoryMessageQueue
    val latch1 = new CountDownLatch(1)
    val latch2 = new CountDownLatch(1)
    val runner = new ActivityRunner(regularActivityQueue = regularActivityQueue,
      delayedActivityQueue = new MemoryMessageQueue, environment = new Environment)
    runner.start()
    regularActivityQueue.put(new Activity {
      override def activate(e: Environment): Seq[Activity] = {
        latch1.countDown()
        Seq(new DelayedActivity {
          override def activate(e: Environment): Seq[Activity] = {
            latch2.countDown()
            Nil
          }
          override def delay: Long = System.currentTimeMillis() + 1000
        })
      }
    })
    latch1.await(10, TimeUnit.SECONDS) shouldBe true
    val t1 = System.currentTimeMillis()
    latch2.await(10, TimeUnit.SECONDS) shouldBe true
    val t2 = System.currentTimeMillis()
    t2 - t1 >= 1000 shouldBe true
    runner.stop()
  }

  it should "run single regular activity which returns two more and they run properly" in {
    val regularActivityQueue = new MemoryMessageQueue
    val latch = new CountDownLatch(3)
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
        }, new Activity {
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
