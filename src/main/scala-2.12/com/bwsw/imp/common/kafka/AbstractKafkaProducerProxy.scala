package com.bwsw.imp.common.kafka

import com.bwsw.imp.message.kafka.KafkaMessage
import org.apache.kafka.clients.producer.ProducerRecord

/**
  * Created by ivan on 04.08.17.
  */
trait AbstractKafkaProducerProxy {
  def sendMessage(record: ProducerRecord[Long, KafkaMessage])
}
