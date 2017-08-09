package com.bwsw.imp.application

import com.bwsw.imp.activity._
import com.bwsw.imp.common.StartStopBehaviour
import com.bwsw.imp.event.EventProcessor
import com.bwsw.imp.message.MessageQueue
import com.bwsw.imp.message.memory.MemoryMessageQueue

/**
  * Created by Ivan Kudryavtsev on 05.08.17.
  */
class SingleNodeRunner extends AbstractRunner with StartStopBehaviour {

  override def start(): Unit = {
    super.start()
    eventProcessor.start()
    activityRunner.start()
  }

  override def stop(): Unit = {
    eventProcessor.stop()
    activityRunner.stop()
    super.stop()
  }

  override protected def buildEventQueue(): MessageQueue = new MemoryMessageQueue

  override protected def buildRegularActivityQueue(): MessageQueue = new MemoryMessageQueue

  override protected def buildDelayedActivityQueue(): MessageQueue = new MemoryMessageQueue

  override protected def buildActivityMatcherRegistry(environment: Environment): ActivityMatcherRegistry = {
    new ActivityMatcherRegistry(environment)
  }

  override protected def buildEstimator(): ActivityEstimator = new PassThroughActivityEstimator

  override protected def buildEventProcessor(): EventProcessor = {
    new EventProcessor(getEventQueue(), getRegularActivityQueue(), buildActivityMatcherRegistry(getEnvironment()), buildEstimator())
  }

  override protected def buildActivityRunner(): ActivityRunner = {
    new ActivityRunner(getRegularActivityQueue(), getDelayedActivityQueue(), getEnvironment())
  }
}
