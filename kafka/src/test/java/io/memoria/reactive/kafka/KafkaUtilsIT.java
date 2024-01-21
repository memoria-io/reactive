package io.memoria.reactive.kafka;

import io.vavr.control.Option;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;

class KafkaUtilsIT {
  private final Infra infra = new Infra("kafka_group");

  @Test
  void topicSize() {
    var conf = infra.kafkaConsumerConfigs();
    var size = KafkaUtils.topicSize("some_topic", 0, conf);
    Assertions.assertEquals(0, size);
  }

  @Test
  void lastKey() {
    var conf = infra.kafkaConsumerConfigs();
    var keyOpt = KafkaUtils.lastKey("some_topic", 0, Duration.ofMillis(300), conf);
    Assertions.assertEquals(Option.none(), keyOpt);
  }
}
