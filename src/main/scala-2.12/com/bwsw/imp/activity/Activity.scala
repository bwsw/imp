package com.bwsw.imp.activity

import com.bwsw.imp.message.MessageQueue

/**
  * Created by Ivan Kudryavtsev on 01.08.17.
  */
abstract class Activity(queue: MessageQueue) extends Runnable with Serializable {

}
