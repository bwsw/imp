package com.bwsw.imp.common.kafka

import com.bwsw.imp.message.Message
import org.apache.kafka.clients.producer.ProducerRecord

/**
  * Created by Ivan Kudryavtsev on 04.08.17.
  */
trait AbstractKafkaProducerProxy {
  def sendMessage(record: ProducerRecord[Long, Message])
}
