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

import static io.memoria.reactive.nats.TestUtils.natsConfig;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NatsESMsgStreamTest {
  private static final Logger log = LoggerFactory.getLogger(NatsESMsgStreamTest.class.getName());
  private static final ESMsgStreamScenario scenario;

  static {
    try {
      String topic = TestsuiteUtils.topicName(NatsESMsgStreamTest.class);
      NatsUtils.createOrUpdateTopic(natsConfig, topic, 1).map(StreamInfo::toString).forEach(log::info);
      var repo = new NatsESMsgStream(natsConfig, TestsuiteUtils.SCHEDULER, TestsuiteUtils.SERIALIZABLE_TRANSFORMER);
      scenario = new ESMsgStreamScenario(TestsuiteUtils.MSG_COUNT, topic, 0, repo);
    } catch (IOException | InterruptedException | JetStreamApiException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  @Order(0)
  void publish() {
    StepVerifier.create(scenario.publish()).expectNextCount(TestsuiteUtils.MSG_COUNT).verifyComplete();
    StepVerifier.create(scenario.last()).expectNext(String.valueOf(TestsuiteUtils.MSG_COUNT - 1)).verifyComplete();
  }

  @Test
  @Order(1)
  void subscribe() {
    StepVerifier.create(scenario.subscribe())
                .expectNextCount(TestsuiteUtils.MSG_COUNT)
                .expectTimeout(TestsuiteUtils.TIMEOUT)
                .verify();
  }

  @Test
  @Order(3)
  void toMessage() {
    var message = NatsESMsgStream.natsMessage("topic", 0, "hello world");
    Assertions.assertEquals("topic_0.subject", message.getSubject());
  }
}
