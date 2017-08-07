package com.bwsw.imp.message.memory

import com.bwsw.imp.message.{FilteredMessageQueue, MessageFilter}

/**
  * Created by Ivan Kudryavtsev on 07.08.17.
  */
class FilteredMemoryMessageQueue(filter: MessageFilter)
  extends FilteredMessageQueue(new MemoryMessageQueue, filter)