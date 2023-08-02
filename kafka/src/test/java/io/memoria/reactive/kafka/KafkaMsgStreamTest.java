package io.memoria.reactive.kafka;

import io.memoria.reactive.core.msg.stream.MsgStream;
import io.memoria.reactive.testsuite.TestsuiteDefaults;
import io.vavr.collection.List;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;

import static io.memoria.reactive.testsuite.TestsuiteDefaults.MSG_COUNT;
import static io.memoria.reactive.testsuite.TestsuiteDefaults.TIMEOUT;

@TestMethodOrder(OrderAnnotation.class)
class KafkaMsgStreamTest {
  private final String topic = TestsuiteDefaults.topicName("messages");
  private final int partition = 0;
  private final MsgStream repo;

  KafkaMsgStreamTest() {
    repo = new KafkaMsgStream(TestUtils.producerConfigs(), TestUtils.consumerConfigs(), Duration.ofMillis(500));
  }

  @Test
  void lastKey() {
    StepVerifier.create(repo.last(topic, partition)).expectComplete().verify();
  }

  @Test
  void publish() {
    // Given
    var msgs = List.range(0, MSG_COUNT).map(TestsuiteDefaults::createEsMsg);
    // When
    var pub = Flux.fromIterable(msgs).concatMap(msg -> repo.pub(topic, partition, msg));
    // Then
    StepVerifier.create(pub).expectNextCount(MSG_COUNT).verifyComplete();
    StepVerifier.create(repo.last(topic, partition)).expectNext(msgs.last()).verifyComplete();
  }

  @Test
  void subscribe() {
    // Given
    var msgs = List.range(0, MSG_COUNT).map(TestsuiteDefaults::createEsMsg);
    var pub = Flux.fromIterable(msgs).concatMap(msg -> repo.pub(topic, partition, msg));

    // When
    var sub = repo.sub(topic, partition);

    // Given
    StepVerifier.create(pub).expectNextCount(MSG_COUNT).verifyComplete();
    StepVerifier.create(sub).expectNextCount(MSG_COUNT).expectTimeout(TIMEOUT).verify();
    StepVerifier.create(sub).expectNextSequence(msgs).expectTimeout(TIMEOUT).verify();
  }
}