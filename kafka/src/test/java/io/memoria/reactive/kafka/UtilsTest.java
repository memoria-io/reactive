package io.memoria.reactive.kafka;

import io.vavr.control.Option;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;

class UtilsTest {

  @Test
  void topicSize() {
    var conf = Infra.consumerConfigs();
    var size = Utils.topicSize("some_topic", 0, conf);
    Assertions.assertEquals(0, size);
  }

  @Test
  void lastKey() {
    var conf = Infra.consumerConfigs();
    var keyOpt = Utils.lastKey("some_topic", 0, Duration.ofMillis(300), conf);
    Assertions.assertEquals(Option.none(), keyOpt);
  }
}
