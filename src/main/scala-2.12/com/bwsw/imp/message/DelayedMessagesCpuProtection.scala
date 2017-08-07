package com.bwsw.imp.message

/**
  * Created by Ivan Kudryavtsev on 07.08.17.
  */
trait DelayedMessagesCpuProtection {
  protected[imp] var cpuProtectionDelay = 0
  protected[imp] var cpuProtectionDelayIncrement = 50
  protected[imp] var cpuProtectionDelayMax = 1000

  def setCpuProtectionParameters(increment: Int, max: Int) = {
    cpuProtectionDelayIncrement = increment
    cpuProtectionDelayMax = max
  }

  protected[imp] def delay() = {
    if(cpuProtectionDelay > 0) Thread.sleep(cpuProtectionDelay)
  }

  protected[imp] def incrementCpuProtectionDelay() = {
    if(cpuProtectionDelay < cpuProtectionDelayMax)
      cpuProtectionDelay += cpuProtectionDelayIncrement
  }

  protected[imp] def resetCpuProtectionDelay() = {
    cpuProtectionDelay = 0
  }

}
