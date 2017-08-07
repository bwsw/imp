package com.bwsw.imp.common.kafka

import com.bwsw.imp.message.Message
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Created by ivan on 04.08.17.
  */
class KafkaProducerProxy(kafkaProducer: KafkaProducer[Long, Message]) extends AbstractKafkaProducerProxy {
  override def sendMessage(record: ProducerRecord[Long, Message]) = {
    kafkaProducer.send(record).get()
  }
}
