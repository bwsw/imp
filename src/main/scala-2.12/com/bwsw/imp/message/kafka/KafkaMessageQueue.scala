package com.bwsw.imp.message.kafka

import com.bwsw.imp.common.kafka.AbstractKafkaProducerProxy
import com.bwsw.imp.message._
import org.apache.curator.framework.CuratorFramework
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.collection.mutable


/**
  * Created by Ivan Kudryavtsev on 01.08.17.
  */



class KafkaMessageQueue(topic: String,
                        consumer: Consumer[Long, Message],
                        producer: AbstractKafkaProducerProxy,
                        offsetKeeper: OffsetKeeper)(implicit curatorClient: CuratorFramework)
  extends MessageQueue with DelayedMessagesCpuProtection {

  protected var offsets = Map[Int, Long]().empty

  private val pollingInterval = MessageQueue.POLLING_INTERVAL

  def saveOffsets() = offsetKeeper.store(topic, offsets)

  override def loadOffsets(): Unit = {
    val partitions = consumer.partitionsFor(topic).iterator().asScala.map(_.partition()).toSet
    val offsets = offsetKeeper.load(topic, partitions)
    offsets.foreach {
      case (partition, offset) => consumer.seek(new TopicPartition(topic, partition), offset + 1)
    }
  }

  override def get: Seq[Message] = {
    delay()

    val records = consumer
      .poll(pollingInterval)
      .records(topic).asScala

    val messages = filterReadyMessages(records)

    setCpuProtectionDelay(records.isEmpty, messages.isEmpty)

    messages
  }

  private def filterReadyMessages(records: Iterable[ConsumerRecord[Long, Message]]): Seq[Message] = {
    val messages = mutable.ListBuffer[Message]()
    offsets ++= records.map(r => {
      if(r.key <= getReadyTime) {
        messages.append(r.value())
      }
      else {
        putInternal(r.value(), r.key())
      }
      r.partition() -> r.offset()
    }).toMap

    messages
  }

  private def setCpuProtectionDelay(receivedMessagesIsEmpty: Boolean, filteredMessagesIsEmpty: Boolean) = {
    if(!receivedMessagesIsEmpty && filteredMessagesIsEmpty) {
      incrementCpuProtectionDelay()
    } else {
      resetCpuProtectionDelay()
    }
  }

  protected def putInternal(message: Message, delay: Long): Unit = {
    val m = new ProducerRecord[Long, Message](topic, delay, message.asInstanceOf[Message])
    producer.sendMessage(m)
  }

  override def put(message: Message): Unit = {
    putInternal(message, 0)
  }

  override def putDelayed(message: DelayedMessage): Unit = putInternal(message.asInstanceOf[Message], message.delay)


}





