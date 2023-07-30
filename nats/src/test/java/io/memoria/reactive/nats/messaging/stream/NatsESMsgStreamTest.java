package io.memoria.reactive.nats.messaging.stream;

import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.reactive.core.messaging.stream.ESMsg;
import io.memoria.reactive.core.messaging.stream.ESMsgStream;
import io.memoria.reactive.nats.NatsConfig;
import io.memoria.reactive.nats.NatsUtils;
import io.memoria.reactive.nats.TestUtils;
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
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.util.Random;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NatsESMsgStreamTest {
  private static final Logger log = LoggerFactory.getLogger(NatsESMsgStreamTest.class.getName());
  private static final Random r = new Random();
  private static final String topic = "topic" + r.nextInt(1000);
  private static final int partition = 0;
  private static final ESMsgStream repo;

  static {
    NatsConfig natsConfig = TestUtils.natsConfig();
    try {
      NatsUtils.createOrUpdateTopic(natsConfig, topic, 1).map(StreamInfo::toString).forEach(log::info);
      repo = new NatsESMsgStream(natsConfig, Schedulers.boundedElastic(), new SerializableTransformer());
    } catch (IOException | InterruptedException | JetStreamApiException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  @Order(0)
  void publish() {
    // Given
    var msgs = List.range(0, TestUtils.COUNT).map(i -> new ESMsg(String.valueOf(i), "hello world"));

    // When
    var pub = Flux.fromIterable(msgs).flatMap(msg -> repo.pub(topic, partition, msg));

    // Then
    StepVerifier.create(pub).expectNextCount(TestUtils.COUNT).verifyComplete();
    StepVerifier.create(repo.last(topic, partition)).expectNext(String.valueOf(TestUtils.COUNT - 1)).verifyComplete();
  }

  @Test
  @Order(1)
  void subscribe() {
    // When
    var sub = repo.sub(topic, partition);

    // Then
    StepVerifier.create(sub).expectNextCount(TestUtils.COUNT).expectTimeout(TestUtils.timeout).verify();
  }

  @Test
  @Order(3)
  void toMessage() {
    var message = NatsESMsgStream.natsMessage("topic", 0, "hello world");
    Assertions.assertEquals("topic_0.subject", message.getSubject());
  }
}
