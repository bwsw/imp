package com.bwsw.imp.message

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 07.08.17.
  */
class DelayedMessageCpuProtectionTests extends FlatSpec with Matchers {
  it should "increment and reset counter as expected" in {
    val delayer = new DelayedMessagesCpuProtection {}
    delayer.cpuProtectionDelay shouldBe 0
    delayer.incrementCpuProtectionDelay()
    delayer.cpuProtectionDelay shouldBe delayer.cpuProtectionDelayIncrement
    delayer.resetCpuProtectionDelay()
    delayer.cpuProtectionDelay shouldBe 0
  }

  it should "not overflow max value" in {
    val delayer = new DelayedMessagesCpuProtection {}
    val max = delayer.cpuProtectionDelayMax
    val inc = delayer.cpuProtectionDelayIncrement
    delayer.cpuProtectionDelay = max - 1
    delayer.incrementCpuProtectionDelay()
    delayer.cpuProtectionDelay shouldBe max - 1 + inc
    delayer.incrementCpuProtectionDelay()
    delayer.cpuProtectionDelay shouldBe max - 1 + inc
  }

  it should "delay" in {
    val delayer = new DelayedMessagesCpuProtection {}
    val inc = delayer.cpuProtectionDelayIncrement
    delayer.incrementCpuProtectionDelay()
    val beforeTime = System.currentTimeMillis()
    delayer.delay()
    val afterTime = System.currentTimeMillis()
    afterTime - beforeTime >= inc shouldBe true
  }

  it should "allow to set parameters" in {
    val delayer = new DelayedMessagesCpuProtection {}
    delayer.setCpuProtectionParameters(1, 10)
    delayer.incrementCpuProtectionDelay()
    delayer.cpuProtectionDelay shouldBe 1
    delayer.cpuProtectionDelayIncrement shouldBe 1
    delayer.cpuProtectionDelayMax shouldBe 10

  }

}
