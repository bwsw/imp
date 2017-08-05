package com.bwsw.imp.activity

import com.bwsw.imp.event.Event

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 01.08.17.
  */
class ActivityMatcherRegistry(environment: Environment) {
  val registry = mutable.ListBuffer[ActivityMatcher]()
  def register(eventFactory: ActivityMatcher) = {
    registry.append(eventFactory)
    this
  }

  def spawn(event: Event): List[Activity] = spawnInt(registry.toList, event)

  private def spawnInt(registry: Seq[ActivityMatcher], event: Event): List[Activity] = {
    registry match {
      case Nil => Nil
      case h :: t => h.spawn(environment, event) ++ spawnInt(t, event)
    }

  }
}
