package com.bwsw.imp.activity

import com.bwsw.imp.common.StartStopBehaviour
import com.bwsw.imp.message.MessageQueue

/**
  * Created by Ivan Kudryavtsev on 07.08.17.
  */
class ActivityRunner(regularActivityQueue: MessageQueue,
                     delayedActivityQueue: DelayedActivity,
                     environment: Environment) extends StartStopBehaviour {

  override def start() = {
    super.start()
  }

  override def stop() = {
    super.stop()
  }

}
