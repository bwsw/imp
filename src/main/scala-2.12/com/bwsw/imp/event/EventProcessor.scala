package com.bwsw.imp.event

import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.imp.activity.{Activity, ActivityMatcherRegistry, DelayedActivity}
import com.bwsw.imp.message.MessageQueue

/**
  * Created by Ivan Kudryavtsev on 05.08.17.
  */
class EventProcessor(eventQueue: MessageQueue,
                     activityQueue: MessageQueue,
                     matcherRegistry: ActivityMatcherRegistry) {
  val exit = new AtomicBoolean(false)
  def poll() = {
    while(!exit.get()) {
      val messages = eventQueue.get
      val activities = matcherRegistry.spawnEvents(messages)
      activities.foreach(activity => activity match {
        case a: DelayedActivity => activityQueue.putDelayed(a)
        case a: Activity => activityQueue.put(a)
        case _ => throw new IllegalArgumentException(s"Unable to handle unknown activity type: ${activity}")
      })
      eventQueue.saveOffsets
    }
  }
}
