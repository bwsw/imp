package com.bwsw.imp.message


/**
  * Created by Ivan Kudryavtsev on 05.08.17.
  */
class DeduplicatingMessageFilter(cache: scala.collection.mutable.Map[String, Boolean]) extends BypassMessageFilter {
  override def filterGet(message: Message): Boolean = super.filterGet(message)

  override def filterPut(message: Message): Boolean = {
    if(cache.contains(message.hash))
      false
    else {
      cache(message.hash) = true
      super.filterPut(message)
    }
  }
}

