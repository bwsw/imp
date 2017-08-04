package com.bwsw.imp.message.kafka

import com.bwsw.imp.kafka.AbstractKafkaProducerProxy
import com.bwsw.imp.message.{Message, Throttler}
import org.apache.curator.framework.CuratorFramework
import org.apache.kafka.clients.consumer.Consumer

/**
  * Created by Ivan Kudryavtsev on 04.08.17.
  */
class ThrottledKafkaQueue(topic: String,
                          consumer: Consumer[Long, KafkaMessage],
                          producer: AbstractKafkaProducerProxy,
                          throttler: Throttler)(implicit curatorClient: CuratorFramework)
  extends KafkaMessageQueue(topic, consumer, producer) {

  override def get: Option[KafkaMessage] = {
    val messageOpt = super.get
    val result = messageOpt.map(message => if(throttler.passEgress(message)) message else null)
    result
  }

  override def put(message: Message, delay: Long): Unit = {
    if(throttler.passIngress(message))
      super.put(message, delay)
  }
}
