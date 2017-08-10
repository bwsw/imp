package com.bwsw.imp.activity

import java.util.concurrent.CountDownLatch

import com.bwsw.imp.common.{Lift, StartStopBehaviour}
import com.bwsw.imp.message.{MessageQueue, MessageReader}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success}

/**
  * Created by Ivan Kudryavtsev on 07.08.17.
  */
class ActivityRunner(regularActivityQueue: MessageQueue,
                     delayedActivityQueue: MessageQueue,
                     environment: Environment) extends StartStopBehaviour {

  val logger = ActivityRunner.logger

  private def pollRegularActivities() = {
    awaitStart.countDown()
    while(!isStopped) {
      pollAndRun(regularActivityQueue, typeTag[Activity])
    }
  }

  private def pollDelayedActivities() = {
    awaitStart.countDown()
    while(!isStopped) {
      pollAndRun(delayedActivityQueue, typeTag[DelayedActivity])
    }
  }

  private def pollAndRun[A <: Activity](queue: MessageReader, cast: TypeTag[A]) = {
    import ExecutionContext.Implicits.global

    val activities = queue.get.map(_.asInstanceOf[A])
    val activityFutures = Lift.waitAll(activities.map(activity => Future { activity.activateIt(environment) })) map {
      res => res.flatMap(f => f match {
        case Success(list) => list
        case Failure(ex) =>
          ActivityMatcherRegistry.logger.error("An exception occurred during running Activity.", ex)
          Nil
      })
    }
    Await.result(activityFutures, Duration.Inf) foreach {
      case a: DelayedActivity => delayedActivityQueue.putDelayed(a)
      case a: Activity => regularActivityQueue.put(a)
    }
    queue.saveOffsets()
  }

  private var awaitStart: CountDownLatch = new CountDownLatch(2)
  private var regularPoller: Thread = _
  private var delayedPoller: Thread = _

  override def start() = {
    super.start()
    logger.info("Activity runner is going to start.")
    awaitStart = new CountDownLatch(2)
    regularPoller = new Thread(() => { pollRegularActivities() })
    delayedPoller = new Thread(() => { pollDelayedActivities() })
    Seq(regularPoller, delayedPoller).map(_.start())
    awaitStart.await()
    logger.info("Activity runner is started.")
  }

  override def stop() = {
    super.stop()
    logger.info("Activity runner is going to stop.")
    Seq(regularPoller, delayedPoller).map(_.join())
    logger.info("Activity runner is stopped.")
  }
}

object ActivityRunner {
  protected val logger = LoggerFactory.getLogger(this.getClass)
}
