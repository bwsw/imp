package com.bwsw.imp.activity

import com.bwsw.imp.common.Lift
import com.bwsw.imp.event.Event
import com.bwsw.imp.message.Message
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * Created by Ivan Kudryavtsev on 01.08.17.
  */
class ActivityMatcherRegistry(environment: Environment) {
  val registry = mutable.ListBuffer[ActivityMatcher]()
  def register(activityMatcher: ActivityMatcher) = {
    registry.append(activityMatcher)
    this
  }

  def spawn(event: Event): Seq[Activity] = spawnInt(registry.toList, event)

  private def spawnInt(registry: Seq[ActivityMatcher], event: Event): Seq[Activity] = {
    ActivityMatcherRegistry.logger.debug(s"Generate activities for $event.")
    registry match {
      case Nil => Nil
      case h :: t => h.spawn(environment, event) ++ spawnInt(t, event)
    }
  }

  def spawnEvents(events: Seq[Message]): Seq[Activity] = {
    import ExecutionContext.Implicits.global

    val activityFutures = Lift.waitAll(events.map(e => Future { spawn(e.asInstanceOf[Event]) })) map {
      res => res.flatMap(f => f match {
        case Success(list) => list
        case Failure(ex) =>
          ActivityMatcherRegistry.logger.error("An exception occurred during Action generation for event.", ex)
          Nil
      })
    }
    Await.result(activityFutures, Duration.Inf)
  }
}

object ActivityMatcherRegistry {
  val logger = LoggerFactory.getLogger(this.getClass)
}
