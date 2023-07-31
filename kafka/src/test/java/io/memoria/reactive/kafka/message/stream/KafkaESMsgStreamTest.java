package io.memoria.reactive.kafka.message.stream;

import io.memoria.reactive.core.message.stream.ESMsgStream;
import io.memoria.reactive.kafka.TestUtils;
import io.memoria.reactive.testsuite.TestsuiteUtils;
import io.vavr.collection.List;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static io.memoria.reactive.testsuite.TestsuiteUtils.MSG_COUNT;
import static io.memoria.reactive.testsuite.TestsuiteUtils.TIMEOUT;

@TestMethodOrder(OrderAnnotation.class)
class KafkaESMsgStreamTest {
  private final String topic = TestsuiteUtils.topicName("messages");
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
    var msgs = List.range(0, MSG_COUNT).map(TestUtils::createEsMsg);
    // When
    var pub = Flux.fromIterable(msgs).concatMap(msg -> repo.pub(topic, partition, msg));
    // Then
    StepVerifier.create(pub).expectNextCount(MSG_COUNT).verifyComplete();
    StepVerifier.create(repo.last(topic, partition)).expectNext(msgs.last().key()).verifyComplete();
  }

  @Test
  void subscribe() {
    // Given
    var msgs = List.range(0, MSG_COUNT).map(TestUtils::createEsMsg);
    var pub = Flux.fromIterable(msgs).concatMap(msg -> repo.pub(topic, partition, msg));

    // When
    var sub = repo.sub(topic, partition);

    // Given
    StepVerifier.create(pub).expectNextCount(MSG_COUNT).verifyComplete();
    StepVerifier.create(sub).expectNextCount(MSG_COUNT).expectTimeout(TIMEOUT).verify();
    StepVerifier.create(sub).expectNextSequence(msgs).expectTimeout(TIMEOUT).verify();
  }
}
