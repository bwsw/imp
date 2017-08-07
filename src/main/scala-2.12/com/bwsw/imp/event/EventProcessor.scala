package com.bwsw.imp.event

import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.imp.activity.{Activity, ActivityMatcherRegistry, DelayedActivity}
import com.bwsw.imp.message.MessageQueue

/**
  * Created by Ivan Kudryavtsev on 05.08.17.
  */
class EventProcessor(eventQueue: MessageQueue,
                     activityQueue: MessageQueue,
                     matcherRegistry: ActivityMatcherRegistry,
                     estimator: Estimator = new PassThroughEstimator) {
  private val exit = new AtomicBoolean(false)
  private def poll() = {
    while(!exit.get()) {
      val messages = eventQueue.get
      val activities = estimator.filter(matcherRegistry.spawnEvents(messages))
      activities.foreach {
        case a: DelayedActivity => activityQueue.putDelayed(a)
        case a: Activity => activityQueue.put(a)
        case activity => throw new IllegalArgumentException(s"Unable to handle unknown activity type: $activity")
      }
      eventQueue.saveOffsets
    }
  }

  var thread: Thread = _
  def start() = {
    if(thread != null)
      throw new IllegalStateException("EventProcessor is already started.")
    thread = new Thread(() => { poll() })
    exit.set(false)
    thread.start()
  }

  def stop() = {
    exit.set(true)
    thread.join()
    thread = null
  }
}
