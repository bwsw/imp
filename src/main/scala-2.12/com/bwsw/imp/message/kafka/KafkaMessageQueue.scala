package com.bwsw.imp.message.kafka

import com.bwsw.imp.common.kafka.AbstractKafkaProducerProxy
import com.bwsw.imp.message.{DelayedMessage, Message, MessageQueue}
import org.apache.curator.framework.CuratorFramework
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.collection.JavaConverters._
import scala.collection.mutable


/**
  * Created by Ivan Kudryavtsev on 01.08.17.
  */

object KafkaMessageQueue {
  private[imp] val POLLING_INTERVAL = 100000
  private[imp] val CPU_PROTECTION_DELAY_INCREMENT = 50
  private[imp] val CPU_PROTECTION_DELAY_MAX = 1000
}

class KafkaMessageQueue(topic: String,
                        consumer: Consumer[Long, KafkaMessage],
                        producer: AbstractKafkaProducerProxy)(implicit curatorClient: CuratorFramework) extends MessageQueue {

  protected var offsets = Map[Int, Long]().empty
  private[imp] var cpuProtectionDelay = 0

  protected def getReadyTime = System.currentTimeMillis()

  def saveOffsets = new OffsetKeeper(topic)(curatorClient).store(offsets)

  override def get: Seq[KafkaMessage] = {
    if(cpuProtectionDelay > 0) Thread.sleep(cpuProtectionDelay)

    val records = consumer
      .poll(KafkaMessageQueue.POLLING_INTERVAL)
      .records(topic).asScala

    val messages = filterReadyMessages(records)

    setCpuProtectionDelay(records.isEmpty, messages.isEmpty)

    messages
  }

  private def filterReadyMessages(records: Iterable[ConsumerRecord[Long, KafkaMessage]]): Seq[KafkaMessage] = {
    val messages = mutable.ListBuffer[KafkaMessage]()
    offsets ++= records.map(r => {
      if(r.key <= getReadyTime) {
        messages.append(r.value())
      }
      else {
        putInternal(r.value(), r.key())
      }
      r.partition() -> r.offset()
    }).toMap

    messages.toSeq
  }

  private def setCpuProtectionDelay(receivedMessagesIsEmpty: Boolean, filteredMessagesIsEmpty: Boolean) = {
    if(!receivedMessagesIsEmpty && filteredMessagesIsEmpty) {
      if(cpuProtectionDelay < KafkaMessageQueue.CPU_PROTECTION_DELAY_MAX) {
        cpuProtectionDelay += KafkaMessageQueue.CPU_PROTECTION_DELAY_INCREMENT
      }
    } else {
      cpuProtectionDelay = 0
    }
  }

  protected def putInternal(message: Message, delay: Long): Unit = {
    val m = new ProducerRecord[Long, KafkaMessage](topic, delay, message.asInstanceOf[KafkaMessage])
    producer.sendMessage(m)
  }

  override def put(message: Message): Unit = {
    putInternal(message, 0)
  }

  override def put(message: DelayedMessage): Unit = putInternal(message.asInstanceOf[KafkaMessage], message.delay)

}





