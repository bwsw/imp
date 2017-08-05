package com.bwsw.imp.message.kafka

import com.bwsw.imp.common.kafka.MockProducerProxy
import com.bwsw.imp.curator.CuratorTests
import com.bwsw.imp.message.{Message, MessageFilter}
import org.apache.kafka.clients.consumer.{ConsumerRecord, MockConsumer, OffsetResetStrategy}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._


/**
  * Created by Ivan Kudryavtsev on 04.08.17.
  */
class ThrottledKafkaMessageQueueTests extends CuratorTests {
  val TOPIC = "sample"
  val PARTITION = 0
  val OFFSET = 1000L

  class TestMessageFilter extends MessageFilter {
    var ingressThrottledCount = 0
    var ingressFilterResult = true
    var egressThrottledCount = 0
    var egressFilterResult = true

    override def filterGet(message: Message): Boolean = {
      ingressThrottledCount += 1
      ingressFilterResult
    }

    override def filterPut(message: Message): Boolean = {
      egressThrottledCount += 1
      egressFilterResult
    }
  }

  it should "throttle ingress and egress messages" in {

    val throttler = new TestMessageFilter

    val consumer = new MockConsumer[Long, KafkaMessage](OffsetResetStrategy.EARLIEST)
    val producer = new MockProducerProxy()
    val message = new KafkaMessage {}

    val mq = new FilteredKafkaMessageQueue(TOPIC, consumer, producer, throttler)

    consumer.assign(Set(new TopicPartition(TOPIC, 0)).asJavaCollection)
    consumer.seek(new TopicPartition(TOPIC, 0), 0)

    throttler.egressFilterResult = true
    consumer.addRecord(new ConsumerRecord[Long, KafkaMessage](TOPIC, PARTITION, OFFSET, 0, message))
    mq.get shouldBe Seq(message)
    throttler.egressThrottledCount shouldBe 1

    throttler.egressFilterResult = false
    consumer.seek(new TopicPartition(TOPIC, 0), 0)
    consumer.addRecord(new ConsumerRecord[Long, KafkaMessage](TOPIC, PARTITION, OFFSET, 0, message))
    mq.get shouldBe Nil
    throttler.egressThrottledCount shouldBe 2

    throttler.egressFilterResult = true
    consumer.seek(new TopicPartition(TOPIC, 0), 0)
    consumer.addRecord(new ConsumerRecord[Long, KafkaMessage](TOPIC, PARTITION, OFFSET, 0, message))
    mq.get shouldBe Seq(message)
    throttler.egressThrottledCount shouldBe 3

    throttler.ingressFilterResult = true
    mq.put(message)
    producer.msgQueue.dequeue()
    throttler.ingressThrottledCount shouldBe 1

    throttler.ingressFilterResult = false
    mq.put(message)
    intercept [NoSuchElementException] {
      producer.msgQueue.dequeue()
    }
    throttler.ingressThrottledCount shouldBe 2

    throttler.ingressFilterResult = false
    mq.put(new KafkaDelayedMessage {
      override def delay: Long = 100
    })
    intercept [NoSuchElementException] {
      producer.msgQueue.dequeue()
    }
    throttler.ingressThrottledCount shouldBe 3

  }
}
