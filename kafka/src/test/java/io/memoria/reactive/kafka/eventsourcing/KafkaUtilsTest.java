package io.memoria.reactive.kafka.eventsourcing;

import io.vavr.control.Option;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;

class KafkaUtilsTest {
  @Test
  void topicSize() {
    var conf = TestUtils.consumerConfigs();
    var size = KafkaUtils.topicSize("some_topic", 0, conf);
    Assertions.assertEquals(0, size);
  }

  @Test
  void lastKey() {
    var conf = TestUtils.consumerConfigs();
    var keyOpt = KafkaUtils.lastKey("some_topic", 0, Duration.ofMillis(300), conf);
    Assertions.assertEquals(Option.none(), keyOpt);
  }
}
