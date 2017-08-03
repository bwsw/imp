package com.bwsw.imp.message.kafka

import com.bwsw.imp.message.{Message, MessageQueue}
import org.apache.curator.framework.CuratorFramework
import org.apache.kafka.clients.consumer.{KafkaConsumer}
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import scala.collection.JavaConverters._

import scala.collection.mutable


/**
  * Created by Ivan Kudryavtsev on 01.08.17.
  */

object KafkaMessageQueue {
  val POLLING_INTERVAL = 1000
}

class KafkaMessageQueue(topic: String,
                        consumer: KafkaConsumer[Int, KafkaMessage],
                        producer: KafkaProducer[Int, KafkaMessage])(implicit curatorClient: CuratorFramework) extends MessageQueue {

  val messages = mutable.Queue[KafkaMessage]()

  override def get: Option[KafkaMessage] = {
    if(messages.isEmpty) {
      val messageRecords = consumer.poll(KafkaMessageQueue.POLLING_INTERVAL)
      val records = messageRecords.records(topic).asScala
      val offsets = records.map(r => {
        messages.enqueue(r.value())
        r.partition() -> r.offset()
      }).toMap

    }

    if(messages.isEmpty)
      None
    else
      Some(messages.dequeue())
  }

  override def put(message: Message): Unit = {
    producer.send(new ProducerRecord[Int, KafkaMessage](topic, 0, message.asInstanceOf[KafkaMessage])).get()
  }

}
