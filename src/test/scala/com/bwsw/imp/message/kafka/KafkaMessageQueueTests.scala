package com.bwsw.imp.message.kafka

import com.bwsw.imp.curator.CuratorTests
import com.bwsw.imp.kafka.MockProducerProxy
import org.apache.kafka.clients.consumer.{ConsumerRecord, MockConsumer, OffsetResetStrategy}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

/**
  * Created by Ivan Kudryavtsev on 03.08.17.
  */
class KafkaMessageQueueTests extends CuratorTests {
  val TOPIC = "sample"
  val PARTITION = 0
  val OFFSET = 1000L
  it should "Fetch ready messages from consumer" in {

    val consumer = new MockConsumer[Long, KafkaMessage](OffsetResetStrategy.EARLIEST)
    val producer = new MockProducerProxy()
    val message = new KafkaMessage {}
    var checkOffsetsAreSaved: Boolean = false

    val mq = new KafkaMessageQueue(TOPIC, consumer, producer) {
      override def saveOffsets = {
        if(checkOffsetsAreSaved)
          offsets(PARTITION) shouldBe OFFSET
      }
    }

    consumer.assign(Set(new TopicPartition(TOPIC, 0)).asJavaCollection)
    consumer.seek(new TopicPartition(TOPIC, 0), 0)
    consumer.addRecord(new ConsumerRecord[Long, KafkaMessage](TOPIC, PARTITION, OFFSET, 0, message))

    checkOffsetsAreSaved = false
    mq.get shouldBe Some(message)

    checkOffsetsAreSaved = true
    mq.get shouldBe None

    checkOffsetsAreSaved = false
    mq.get shouldBe None

    checkOffsetsAreSaved = false
    consumer.addRecord(new ConsumerRecord[Long, KafkaMessage](TOPIC, PARTITION, OFFSET+1, 0, message))
    consumer.addRecord(new ConsumerRecord[Long, KafkaMessage](TOPIC, PARTITION, OFFSET+2, 0, message))
    mq.get shouldBe Some(message)
    mq.get shouldBe Some(message)

  }

  it should "forward future messages to producer" in {
    val consumer = new MockConsumer[Long, KafkaMessage](OffsetResetStrategy.EARLIEST)
    val producer = new MockProducerProxy()
    val message = new KafkaMessage {}
    var nowTime = 100

    val mq = new KafkaMessageQueue(TOPIC, consumer, producer) {
      override def getReadyTime = {
        nowTime
      }
    }

    consumer.assign(Set(new TopicPartition(TOPIC, 0)).asJavaCollection)
    consumer.seek(new TopicPartition(TOPIC, 0), 0)
    consumer.addRecord(new ConsumerRecord[Long, KafkaMessage](TOPIC, PARTITION, OFFSET, nowTime + 1, message))
    mq.get shouldBe None
    val record = producer.msgQueue.dequeue()
    record.key() shouldBe nowTime + 1
    record.value() shouldBe message

    consumer.addRecord(new ConsumerRecord[Long, KafkaMessage](TOPIC, PARTITION, OFFSET + 1, nowTime + 1, message))
    nowTime += 1
    mq.get shouldBe Some(message)
    producer.msgQueue.isEmpty shouldBe true
    mq.get shouldBe None
    producer.msgQueue.isEmpty shouldBe true

    val offsetKeeper = new OffsetKeeper(TOPIC)
    offsetKeeper.load(Set(PARTITION)) shouldBe Map (PARTITION -> (OFFSET + 1))
  }

  it should "increase cpu protection delay on unsuccessful read if 0 objects passed the filter" in {
    val consumer = new MockConsumer[Long, KafkaMessage](OffsetResetStrategy.EARLIEST)
    val producer = new MockProducerProxy()
    val message = new KafkaMessage {}
    var nowTime = 100

    val mq = new KafkaMessageQueue(TOPIC, consumer, producer) {
      override def getReadyTime = {
        nowTime
      }
    }

    consumer.assign(Set(new TopicPartition(TOPIC, 0)).asJavaCollection)
    consumer.seek(new TopicPartition(TOPIC, 0), 0)
    consumer.addRecord(new ConsumerRecord[Long, KafkaMessage](TOPIC, PARTITION, OFFSET, nowTime + 1, message))
    mq.get shouldBe None

    mq.cpuProtectionDelay shouldBe KafkaMessageQueue.CPU_PROTECTION_DELAY_INCREMENT

    consumer.addRecord(new ConsumerRecord[Long, KafkaMessage](TOPIC, PARTITION, OFFSET + 1, nowTime + 1, message))
    mq.get shouldBe None
    mq.cpuProtectionDelay shouldBe KafkaMessageQueue.CPU_PROTECTION_DELAY_INCREMENT * 2

    consumer.addRecord(new ConsumerRecord[Long, KafkaMessage](TOPIC, PARTITION, OFFSET + 2, nowTime + 1, message))
    consumer.addRecord(new ConsumerRecord[Long, KafkaMessage](TOPIC, PARTITION, OFFSET + 3, nowTime + 1, message))

    nowTime += 1
    mq.get shouldBe Some(message)
    mq.cpuProtectionDelay shouldBe 0
    mq.get shouldBe Some(message)
    mq.cpuProtectionDelay shouldBe 0

  }

  it should "allow put DelayedMessage properly" in {
    val consumer = new MockConsumer[Long, KafkaMessage](OffsetResetStrategy.EARLIEST)
    val producer = new MockProducerProxy()
    val message = new KafkaDelayedMessage {
      override def delay: Long = 1000
    }

    val mq = new KafkaMessageQueue(TOPIC, consumer, producer)
    mq.put(message)
  }
}
