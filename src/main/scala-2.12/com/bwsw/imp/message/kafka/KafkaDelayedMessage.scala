package com.bwsw.imp.message.kafka

import com.bwsw.imp.message.DelayedMessage

/**
  * Created by Ivan Kudryavtsev on 04.08.17.
  */
abstract class KafkaDelayedMessage extends KafkaMessage with DelayedMessage {

}
