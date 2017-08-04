package com.bwsw.imp.kafka

import com.bwsw.imp.message.kafka.KafkaMessage
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Created by ivan on 04.08.17.
  */
class KafkaProducerProxy(kafkaProducer: KafkaProducer[Long, KafkaMessage]) extends AbstractKafkaProducerProxy {
  override def sendMessage(record: ProducerRecord[Long, KafkaMessage]) = {
    kafkaProducer.send(record).get()
  }
}
