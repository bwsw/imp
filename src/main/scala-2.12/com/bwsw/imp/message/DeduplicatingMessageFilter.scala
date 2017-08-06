package com.bwsw.imp.message


/**
  * Created by Ivan Kudryavtsev on 05.08.17.
  */
class DeduplicatingMessageFilter(cache: MessageCache) extends BypassMessageFilter {
  override def filterGet(message: Message): Boolean = super.filterGet(message)

  override def filterPut(message: Message): Boolean = {
    if(cache.checkExists(message))
      false
    else {
      super.filterPut(message)
    }
  }
}

