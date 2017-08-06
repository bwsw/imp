package com.bwsw.imp.message

/**
  * Created by Ivan Kudryavtsev on 06.08.17.
  */
abstract class MessageCache {
  def checkExists(m: Message): Boolean
}
