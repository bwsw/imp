package com.bwsw.imp.common.kafka

import com.bwsw.imp.message.kafka.KafkaMessage
import org.apache.kafka.clients.producer.ProducerRecord

import scala.collection.mutable

/**
  * Created by ivan on 04.08.17.
  */
class MockProducerProxy() extends AbstractKafkaProducerProxy {
  val msgQueue = mutable.Queue[ProducerRecord[Long, KafkaMessage]]()
  override def sendMessage(record: ProducerRecord[Long, KafkaMessage]): Unit = {
    msgQueue.enqueue(record)
  }
}
