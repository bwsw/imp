package com.bwsw.imp.event

import com.bwsw.imp.activity.{Activity, ActivityEstimator, ActivityMatcherRegistry, DelayedActivity}
import com.bwsw.imp.common.StartStopBehaviour
import com.bwsw.imp.message.{MessageReader, MessageWriter}
import org.slf4j.LoggerFactory

/**
  * Created by Ivan Kudryavtsev on 05.08.17.
  */
class EventProcessor(eventQueue: MessageReader,
                     activityQueue: MessageWriter,
                     activityMatcherRegistry: ActivityMatcherRegistry,
                     estimator: ActivityEstimator) extends StartStopBehaviour {
  val logger = EventProcessor.logger
  private def poll() = {
    while(!isStopped) {
      val messages = eventQueue.get
      val activities = estimator.filter(activityMatcherRegistry.spawnEvents(messages))
      activities.foreach {
        case a: DelayedActivity =>
          logger.debug(s"Event processor put delayed activity $a to activity queue.")

          activityQueue.putDelayed(a)
        case a: Activity =>
          logger.debug(s"Event processor put regular activity $a to activity queue.")

          activityQueue.put(a)
      }

      logger.debug(s"Event processor is going to save offsets.")

      eventQueue.saveOffsets()

      logger.debug(s"Event processor have saved offsets.")
    }
  }

  var thread: Thread = _
  override def start() = {
    logger.info("Event processor is going to start.")
    super.start()
    thread = new Thread(() => { poll() })
    thread.start()
    logger.info("Event processor is started.")
  }

  override def stop() = {
    logger.info("Event processor is going to stop.")
    super.stop()
    thread.join()
    thread = null
    logger.info("Event processor is stopped.")
  }
}

object EventProcessor {
  protected val logger = LoggerFactory.getLogger(this.getClass)
}