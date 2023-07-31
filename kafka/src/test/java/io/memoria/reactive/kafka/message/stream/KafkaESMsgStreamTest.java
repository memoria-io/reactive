package io.memoria.reactive.kafka.message.stream;

import io.memoria.reactive.core.message.stream.ESMsgStream;
import io.memoria.reactive.kafka.TestUtils;
import io.vavr.collection.List;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Random;

@TestMethodOrder(OrderAnnotation.class)
class KafkaESMsgStreamTest {
  private static final Duration timeout = Duration.ofMillis(500);
  private static final Random random = new Random();
  private static final int MSG_COUNT = 1000;
  private final String topic = "node" + random.nextInt(1000);
  private final int partition = 0;
  private final ESMsgStream repo;

  KafkaESMsgStreamTest() {
    repo = new KafkaESMsgStream(TestUtils.producerConfigs(), TestUtils.consumerConfigs());
  }

  @Test
  void lastKey() {
    StepVerifier.create(repo.last(topic, partition)).expectComplete().verify();
  }

  @Test
  void publish() {
    // Given
    var msgs = List.range(0, MSG_COUNT).map(i -> TestUtils.createEsMsg(i));
    // When
    var pub = Flux.fromIterable(msgs).concatMap(msg -> repo.pub(topic, partition, msg));
    // Then
    StepVerifier.create(pub).expectNextCount(MSG_COUNT).verifyComplete();
    StepVerifier.create(repo.last(topic, partition)).expectNext(msgs.last().key()).verifyComplete();
  }

  @Test
  void subscribe() {
    // Given
    var msgs = List.range(0, MSG_COUNT).map(i -> TestUtils.createEsMsg(i));
    var pub = Flux.fromIterable(msgs).concatMap(msg -> repo.pub(topic, partition, msg));

    // When
    var sub = repo.sub(topic, partition);

    // Given
    StepVerifier.create(pub).expectNextCount(MSG_COUNT).verifyComplete();
    StepVerifier.create(sub).expectNextCount(MSG_COUNT).expectTimeout(timeout).verify();
    StepVerifier.create(sub).expectNextSequence(msgs).expectTimeout(timeout).verify();
  }
}
