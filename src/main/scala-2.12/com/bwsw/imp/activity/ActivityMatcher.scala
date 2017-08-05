package com.bwsw.imp.activity

import com.bwsw.imp.event.Event

/**
  * Created by Ivan Kudryavtsev on 01.08.17.
  */
trait ActivityMatcher {
  def spawn(environment: Environment, event: Event): List[Activity]
}
