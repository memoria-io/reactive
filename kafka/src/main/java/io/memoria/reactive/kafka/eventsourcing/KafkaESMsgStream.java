package io.memoria.reactive.kafka.eventsourcing;

import io.memoria.reactive.core.stream.ESMsgStream;
import io.vavr.collection.Map;
import reactor.kafka.sender.KafkaSender;

public interface KafkaESMsgStream extends ESMsgStream {
  static KafkaESMsgStream create(Map<String, Object> producerConfig,
                                 Map<String, Object> consumerConfig,
                                 KafkaSender<String, String> sender) {
    return new DefaultKafkaESMsgStream(producerConfig, consumerConfig, sender);
  }
}
