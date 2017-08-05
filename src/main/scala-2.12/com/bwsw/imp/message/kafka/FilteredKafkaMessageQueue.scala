package com.bwsw.imp.message.kafka

import com.bwsw.imp.common.kafka.AbstractKafkaProducerProxy
import com.bwsw.imp.message.{DelayedMessage, Message, MessageFilter}
import org.apache.curator.framework.CuratorFramework
import org.apache.kafka.clients.consumer.Consumer

/**
  * Created by Ivan Kudryavtsev on 04.08.17.
  */
class FilteredKafkaMessageQueue(topic: String,
                                consumer: Consumer[Long, KafkaMessage],
                                producer: AbstractKafkaProducerProxy,
                                throttler: MessageFilter)(implicit curatorClient: CuratorFramework)
  extends KafkaMessageQueue(topic, consumer, producer) {

  override def get: Seq[KafkaMessage] = {
    val messages = super.get
    val result = messages.flatMap(message => if(throttler.filterPut(message)) Seq(message) else Seq.empty)
    result
  }

  override def put(message: Message): Unit = {
    if(throttler.filterGet(message))
      super.put(message)
  }

  override def putDelayed(message: DelayedMessage): Unit = {
    if(throttler.filterGet(message))
      super.putDelayed(message)
  }
}
