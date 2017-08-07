package com.bwsw.imp.event

import com.bwsw.imp.activity.{Activity, ActivityMatcherRegistry, DelayedActivity}
import com.bwsw.imp.common.StartStopBehaviour
import com.bwsw.imp.message.{MessageReader, MessageWriter}
import org.slf4j.LoggerFactory

/**
  * Created by Ivan Kudryavtsev on 05.08.17.
  */
class EventProcessor(eventQueue: MessageReader,
                     activityQueue: MessageWriter,
                     activityMatcherRegistry: ActivityMatcherRegistry,
                     estimator: Estimator) extends StartStopBehaviour {
  private def poll() = {
    while(!isStopped) {
      val messages = eventQueue.get
      val activities = estimator.filter(activityMatcherRegistry.spawnEvents(messages))
      activities.foreach {
        case a: DelayedActivity =>
          if(EventProcessor.logger.isDebugEnabled)
            EventProcessor.logger.debug(s"Event processor put delayed activity $a to activity queue.")

          activityQueue.putDelayed(a)
        case a: Activity =>
          if(EventProcessor.logger.isDebugEnabled)
            EventProcessor.logger.debug(s"Event processor put regular activity $a to activity queue.")

          activityQueue.put(a)
        case activity => throw new IllegalArgumentException(s"Unable to handle unknown activity type: $activity")
      }

      if(EventProcessor.logger.isDebugEnabled)
        EventProcessor.logger.debug(s"Event processor is going to save offsets.")

      eventQueue.saveOffsets()

      if(EventProcessor.logger.isDebugEnabled)
        EventProcessor.logger.debug(s"Event processor have saved offsets.")
    }
  }

  var thread: Thread = _
  override def start() = {
    EventProcessor.logger.info("Event processor is going to start.")
    super.start()
    if(thread != null)
      throw new IllegalStateException("EventProcessor is already started.")
    thread = new Thread(() => { poll() })
    thread.start()
    EventProcessor.logger.info("Event processor is started.")
  }

  override def stop() = {
    EventProcessor.logger.info("Event processor is going to stop.")
    super.stop()
    thread.join()
    thread = null
    EventProcessor.logger.info("Event processor is stopped.")
  }
}

object EventProcessor {
  val logger = LoggerFactory.getLogger(this.getClass)
}