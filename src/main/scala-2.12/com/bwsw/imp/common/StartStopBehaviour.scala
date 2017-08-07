package com.bwsw.imp.common

import java.util.concurrent.atomic.AtomicBoolean

/**
  * Created by Ivan Kudryavtsev on 07.08.17.
  */
abstract class StartStopBehaviour {
  private val isStoppedFlag = new AtomicBoolean(true)
  def start() = {
    if(!isStoppedFlag.getAndSet(false))
      throw new IllegalStateException(s"Object $this is already started. Next start is available only after stop call.")
  }
  def stop() = {
    if(isStoppedFlag.getAndSet(true))
      throw new IllegalStateException(s"Object $this is already stopped. Next stop is available only after start call.")
  }

  def isStopped = isStoppedFlag.get()
}
