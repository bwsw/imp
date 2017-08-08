package com.bwsw.imp.activity

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
}
