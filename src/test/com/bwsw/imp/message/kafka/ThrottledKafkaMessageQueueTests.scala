package com.bwsw.imp.message.kafka

import com.bwsw.cloudstack.common.curator.CuratorTests
import com.bwsw.imp.kafka.MockProducerProxy
import com.bwsw.imp.message.{Throttler, Message}
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetResetStrategy, MockConsumer}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._


/**
  * Created by Ivan Kudryavtsev on 04.08.17.
  */
class ThrottledKafkaMessageQueueTests extends CuratorTests {
  val TOPIC = "sample"
  val PARTITION = 0
  val OFFSET = 1000L

  class TestThrottler extends Throttler {
    var ingressThrottledCount = 0
    var ingressReturn = true
    var egressThrottledCount = 0
    var egressReturn = true

    override def passIngress(message: Message): Boolean = {
      ingressThrottledCount += 1
      ingressReturn
    }

    override def passEgress(message: Message): Boolean = {
      egressThrottledCount += 1
      egressReturn
    }
  }

  it should "throttle ingress and egress messages" in {

    val throttler = new TestThrottler

    val consumer = new MockConsumer[Long, KafkaMessage](OffsetResetStrategy.EARLIEST)
    val producer = new MockProducerProxy()
    val message = new KafkaMessage {}

    val mq = new ThrottledKafkaMessageQueue(TOPIC, consumer, producer, throttler)

    consumer.assign(Set(new TopicPartition(TOPIC, 0)).asJavaCollection)
    consumer.seek(new TopicPartition(TOPIC, 0), 0)

    throttler.egressReturn = true
    consumer.addRecord(new ConsumerRecord[Long, KafkaMessage](TOPIC, PARTITION, OFFSET, 0, message))
    mq.get shouldBe Some(message)
    throttler.egressThrottledCount shouldBe 1

    throttler.egressReturn = false
    consumer.seek(new TopicPartition(TOPIC, 0), 0)
    consumer.addRecord(new ConsumerRecord[Long, KafkaMessage](TOPIC, PARTITION, OFFSET, 0, message))
    mq.get shouldBe None
    throttler.egressThrottledCount shouldBe 2

    throttler.egressReturn = true
    consumer.seek(new TopicPartition(TOPIC, 0), 0)
    consumer.addRecord(new ConsumerRecord[Long, KafkaMessage](TOPIC, PARTITION, OFFSET, 0, message))
    mq.get shouldBe Some(message)
    throttler.egressThrottledCount shouldBe 3

    throttler.ingressReturn = true
    mq.put(message)
    producer.msgQueue.dequeue()
    throttler.ingressThrottledCount shouldBe 1

    throttler.ingressReturn = false
    mq.put(message)
    intercept [NoSuchElementException] {
      producer.msgQueue.dequeue()
    }
    throttler.ingressThrottledCount shouldBe 2

    throttler.ingressReturn = false
    mq.put(new KafkaDelayedMessage {
      override def delay: Long = 100
    })
    intercept [NoSuchElementException] {
      producer.msgQueue.dequeue()
    }
    throttler.ingressThrottledCount shouldBe 3

  }
}
