package com.bwsw.imp.activity

import com.bwsw.imp.event.Event

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 01.08.17.
  */
class ActivityMatcherRegistry {
  val registry = mutable.ListBuffer[ActivityMatcher]()
  def register(eventFactory: ActivityMatcher) = {
    registry.append(eventFactory)
    this
  }

  def generate(event: Event): List[Activity] = generateInt(registry.toList, event)
  private def generateInt(registry: Seq[ActivityMatcher], event: Event): List[Activity] = {
    registry match {
      case Nil => Nil
      case h :: t => h.generate(event) ++ generateInt(t, event)
    }

  }
}
