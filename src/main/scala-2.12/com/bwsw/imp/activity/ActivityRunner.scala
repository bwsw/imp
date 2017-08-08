package com.bwsw.imp.activity

import java.util.concurrent.CountDownLatch

import com.bwsw.imp.common.StartStopBehaviour
import com.bwsw.imp.message.MessageQueue

/**
  * Created by Ivan Kudryavtsev on 07.08.17.
  */
class ActivityRunner(regularActivityQueue: MessageQueue,
                     delayedActivityQueue: MessageQueue,
                     environment: Environment) extends StartStopBehaviour {

  def pollRegularActivities() = {
    awaitStart.countDown()
    while(!isStopped) {
      val activities = regularActivityQueue.get.map(_.asInstanceOf[Activity])
    }
  }

  def pollDelayedActivities() = {
    awaitStart.countDown()
    while(!isStopped) {
      val delayedActivities = regularActivityQueue.get.map(_.asInstanceOf[DelayedActivity])
    }
  }

  private var awaitStart: CountDownLatch = new CountDownLatch(2)
  private var regularPoller: Thread = _
  private var delayedPoller: Thread = _

  override def start() = {
    super.start()
    awaitStart = new CountDownLatch(2)
    regularPoller = new Thread(() => { pollRegularActivities() })
    delayedPoller = new Thread(() => { pollDelayedActivities() })
    Seq(regularPoller, delayedPoller).map(_.start())
    awaitStart.await()
  }

  override def stop() = {
    super.stop()
    Seq(regularPoller, delayedPoller).map(_.join())
  }

}
