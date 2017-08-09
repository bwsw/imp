package com.bwsw.imp.message.kafka

import com.bwsw.imp.common.kafka.MockProducerProxy
import com.bwsw.imp.curator.CuratorTests
import com.bwsw.imp.message.kafka.offsets.ZookeeperOffsetKeeper
import com.bwsw.imp.message.{DelayedMessage, Message, MessageFilter}
import org.apache.kafka.clients.consumer.{ConsumerRecord, MockConsumer, OffsetResetStrategy}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._


/**
  * Created by Ivan Kudryavtsev on 04.08.17.
  */
class FilteredKafkaMessageQueueTests extends CuratorTests {
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

    val filter = new TestMessageFilter

    val consumer = new MockConsumer[Long, Message](OffsetResetStrategy.EARLIEST)
    val producer = new MockProducerProxy()
    val message = new Message {}

    val mq = new FilteredKafkaMessageQueue(TOPIC, consumer, producer, filter, new ZookeeperOffsetKeeper)

    consumer.assign(Set(new TopicPartition(TOPIC, 0)).asJavaCollection)
    consumer.seek(new TopicPartition(TOPIC, 0), 0)

    filter.egressFilterResult = true
    consumer.addRecord(new ConsumerRecord[Long, Message](TOPIC, PARTITION, OFFSET, 0, message))
    mq.get shouldBe Seq(message)
    filter.egressThrottledCount shouldBe 1

    filter.egressFilterResult = false
    consumer.seek(new TopicPartition(TOPIC, 0), 0)
    consumer.addRecord(new ConsumerRecord[Long, Message](TOPIC, PARTITION, OFFSET, 0, message))
    mq.get shouldBe Nil
    filter.egressThrottledCount shouldBe 2

    filter.egressFilterResult = true
    consumer.seek(new TopicPartition(TOPIC, 0), 0)
    consumer.addRecord(new ConsumerRecord[Long, Message](TOPIC, PARTITION, OFFSET, 0, message))
    mq.get shouldBe Seq(message)
    filter.egressThrottledCount shouldBe 3

    filter.ingressFilterResult = true
    mq.put(message)
    producer.msgQueue.dequeue()
    filter.ingressThrottledCount shouldBe 1

    filter.ingressFilterResult = false
    mq.put(message)
    intercept [NoSuchElementException] {
      producer.msgQueue.dequeue()
    }
    filter.ingressThrottledCount shouldBe 2

    filter.ingressFilterResult = false
    mq.putDelayed(new DelayedMessage {
      override def delay: Long = 100
    })
    intercept [NoSuchElementException] {
      producer.msgQueue.dequeue()
    }
    filter.ingressThrottledCount shouldBe 3

  }
}
