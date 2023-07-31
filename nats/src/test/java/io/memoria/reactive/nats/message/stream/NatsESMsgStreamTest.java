package io.memoria.reactive.nats.message.stream;

import io.memoria.reactive.nats.NatsUtils;
import io.memoria.reactive.testsuite.TestsuiteUtils;
import io.memoria.reactive.testsuite.message.stream.ESMsgStreamScenario;
import io.nats.client.JetStreamApiException;
import io.nats.client.api.StreamInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.test.StepVerifier;

import java.io.IOException;

import static io.memoria.reactive.nats.TestUtils.NATS_CONFIG;
import static io.memoria.reactive.testsuite.TestsuiteUtils.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NatsESMsgStreamTest {
  private static final Logger log = LoggerFactory.getLogger(NatsESMsgStreamTest.class.getName());
  private static final ESMsgStreamScenario scenario;

  static {
    try {
      String topic = TestsuiteUtils.topicName(NatsESMsgStreamTest.class);
      NatsUtils.createOrUpdateTopic(NATS_CONFIG, topic, 1).map(StreamInfo::toString).forEach(log::info);
      var repo = new NatsESMsgStream(NATS_CONFIG, TRANSFORMER, SCHEDULER);
      scenario = new ESMsgStreamScenario(MSG_COUNT, topic, 0, repo);
    } catch (IOException | InterruptedException | JetStreamApiException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  @Order(0)
  void publish() {
    StepVerifier.create(scenario.publish()).expectNextCount(MSG_COUNT).verifyComplete();
    StepVerifier.create(scenario.last()).expectNext(String.valueOf(MSG_COUNT - 1)).verifyComplete();
  }

  @Test
  @Order(1)
  void subscribe() {
    StepVerifier.create(scenario.subscribe()).expectNextCount(MSG_COUNT).expectTimeout(TIMEOUT).verify();
  }

  @Test
  @Order(3)
  void toMessage() {
    var message = NatsESMsgStream.natsMessage("topic", 0, "hello world");
    Assertions.assertEquals("topic_0.subject", message.getSubject());
  }
}
