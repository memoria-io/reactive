package io.memoria.reactive.nats.message.stream;

import io.memoria.reactive.core.message.stream.ESMsg;
import io.memoria.reactive.core.message.stream.ESMsgStream;
import io.memoria.reactive.nats.NatsUtils;
import io.memoria.reactive.testsuite.TestsuiteUtils;
import io.nats.client.JetStreamApiException;
import io.nats.client.api.StreamInfo;
import io.vavr.collection.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.io.IOException;

import static io.memoria.reactive.nats.TestUtils.natsConfig;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NatsESMsgStreamTest {
  private static final Logger log = LoggerFactory.getLogger(NatsESMsgStreamTest.class.getName());
  private static final String topic = TestsuiteUtils.topicName(NatsESMsgStreamTest.class);
  private static final int partition = 0;
  private static final ESMsgStream repo;

  static {
    try {
      NatsUtils.createOrUpdateTopic(natsConfig, topic, 1).map(StreamInfo::toString).forEach(log::info);
      repo = new NatsESMsgStream(natsConfig, TestsuiteUtils.SCHEDULER, TestsuiteUtils.SERIALIZABLE_TRANSFORMER);
    } catch (IOException | InterruptedException | JetStreamApiException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  @Order(0)
  void publish() {
    // Given
    var msgs = List.range(0, TestsuiteUtils.MSG_COUNT).map(i -> new ESMsg(String.valueOf(i), "hello world"));

    // When
    var pub = Flux.fromIterable(msgs).flatMap(msg -> repo.pub(topic, partition, msg));

    // Then
    StepVerifier.create(pub).expectNextCount(TestsuiteUtils.MSG_COUNT).verifyComplete();
    StepVerifier.create(repo.last(topic, partition))
                .expectNext(String.valueOf(TestsuiteUtils.MSG_COUNT - 1))
                .verifyComplete();
  }

  @Test
  @Order(1)
  void subscribe() {
    // When
    var sub = repo.sub(topic, partition);

    // Then
    StepVerifier.create(sub).expectNextCount(TestsuiteUtils.MSG_COUNT).expectTimeout(TestsuiteUtils.TIMEOUT).verify();
  }

  @Test
  @Order(3)
  void toMessage() {
    var message = NatsESMsgStream.natsMessage("topic", 0, "hello world");
    Assertions.assertEquals("topic_0.subject", message.getSubject());
  }
}
