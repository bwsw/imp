package com.bwsw.imp.message.kafka

import com.bwsw.imp.common.kafka.AbstractKafkaProducerProxy
import com.bwsw.imp.message.{FilteredMessageQueue, Message, MessageFilter, OffsetKeeper}
import org.apache.curator.framework.CuratorFramework
import org.apache.kafka.clients.consumer.Consumer

/**
  * Created by Ivan Kudryavtsev on 04.08.17.
  */
class FilteredKafkaMessageQueue(topic: String,
                                consumer: Consumer[Long, Message],
                                producer: AbstractKafkaProducerProxy,
                                filter: MessageFilter, offsetKeeper: OffsetKeeper)(implicit curatorClient: CuratorFramework)
  extends FilteredMessageQueue(new KafkaMessageQueue(topic, consumer, producer, offsetKeeper), filter)