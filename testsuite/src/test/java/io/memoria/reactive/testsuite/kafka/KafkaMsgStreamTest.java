package io.memoria.reactive.testsuite.kafka;

import io.memoria.reactive.core.stream.Msg;
import io.memoria.reactive.kafka.KafkaMsgStream;
import io.memoria.reactive.testsuite.Infra;
import io.memoria.reactive.testsuite.MsgStreamScenario;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import reactor.test.StepVerifier;

import java.time.Duration;

import static io.memoria.reactive.testsuite.Infra.MSG_COUNT;
import static io.memoria.reactive.testsuite.Infra.TIMEOUT;

@TestMethodOrder(OrderAnnotation.class)
class KafkaMsgStreamTest {
  private static final String topic = Infra.topicName("messages");
  private static final MsgStreamScenario scenario;

  static {
    var repo = new KafkaMsgStream(KafkaInfra.producerConfigs(), KafkaInfra.consumerConfigs(), Duration.ofMillis(500));
    scenario = new MsgStreamScenario(MSG_COUNT, topic, 0, repo);
  }

  @Test
  @Order(0)
  void publish() {
    var now = System.currentTimeMillis();
    StepVerifier.create(scenario.publish()).expectNextCount(MSG_COUNT).verifyComplete();
    Infra.printRates("publish", now);
  }

  @Test
  @Order(1)
  void subscribe() {
    var now = System.currentTimeMillis();
    StepVerifier.create(scenario.subscribe()).expectNextCount(MSG_COUNT).expectTimeout(TIMEOUT).verify();
    Infra.printRates("subscribe", now);
  }

  @Test
  @Order(2)
  void last() {
    StepVerifier.create(scenario.last().map(Msg::key)).expectNext(String.valueOf(MSG_COUNT - 1)).verifyComplete();
  }
}
