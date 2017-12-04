package com.bwsw.imp.application

import com.bwsw.imp.activity._
import com.bwsw.imp.event.EventProcessor
import com.bwsw.imp.message.{MessageQueue, MessageWriter}

/**
  * Created by Ivan Kudryavtsev on 09.08.17.
  */
abstract class AbstractRunner {
  //todo: protected and public methods required type annotation

  protected val eventQueue = buildEventQueue()

  private val regularActivityQueue = buildRegularActivityQueue()
  private val delayedActivityQueue = buildDelayedActivityQueue()
  private val matcherRegistry = buildActivityMatcherRegistry(getEnvironment())

  protected val eventProcessor = buildEventProcessor()
  protected val activityRunner = buildActivityRunner()

  def getEventQueue() = eventQueue
  protected def getRegularActivityQueue() = regularActivityQueue
  protected def getDelayedActivityQueue() = delayedActivityQueue
  protected def getActivityMatcherRegistry() = matcherRegistry

  //builders
  protected def buildEventQueue(): MessageQueue
  protected def buildRegularActivityQueue(): MessageQueue
  protected def buildActivityMatcherRegistry(environment: Environment): ActivityMatcherRegistry
  protected def buildEstimator(): ActivityEstimator
  protected def buildEventProcessor(): EventProcessor
  protected def buildActivityRunner(): ActivityRunner
  protected def buildDelayedActivityQueue(): MessageQueue

  protected def getEnvironment(): Environment = new Environment {
    val eventWriter = eventQueue.asInstanceOf[MessageWriter]
  }

  def registerActivityMatcher(matcher: ActivityMatcher) = matcherRegistry.register(matcher)
}
