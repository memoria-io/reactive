package io.memoria.reactive.kafka.eventsourcing;

import io.memoria.reactive.core.stream.ESMsgStream;
import io.vavr.collection.List;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Random;

@TestMethodOrder(OrderAnnotation.class)
class DefaultKafkaESMsgStreamTest {
  private static final Random random = new Random();
  private static final int MSG_COUNT = 1000;
  private final String topic = "node" + random.nextInt(1000);
  private final int partition = 0;
  private final ESMsgStream repo;

  DefaultKafkaESMsgStreamTest() {
    var sender = KafkaUtils.createSender(TestUtils.producerConfigs());
    repo = KafkaESMsgStream.create(TestUtils.producerConfigs(), TestUtils.consumerConfigs(), sender);
  }

  @Test
  void lastKey() {
    StepVerifier.create(repo.last(topic, partition)).expectComplete().verify();
  }

  @Test
  void publish() {
    // Given
    var msgs = List.range(0, MSG_COUNT).map(i -> TestUtils.createEsMsg(topic, partition, i));
    // When
    var pub = Flux.fromIterable(msgs).concatMap(repo::pub);
    // Then
    StepVerifier.create(pub).expectNextCount(MSG_COUNT).verifyComplete();
    StepVerifier.create(repo.last(topic, partition)).expectNext(msgs.last().key()).verifyComplete();
  }

  @Test
  void subscribe() {
    // Given
    var msgs = List.range(0, MSG_COUNT).map(i -> TestUtils.createEsMsg(topic, partition, i));
    var pub = Flux.fromIterable(msgs).concatMap(repo::pub);

    // When
    var sub = repo.sub(topic, partition).take(MSG_COUNT);

    // Given
    StepVerifier.create(pub).expectNextCount(MSG_COUNT).verifyComplete();
    StepVerifier.create(sub).expectNextCount(MSG_COUNT).verifyComplete();
    StepVerifier.create(sub).expectNextSequence(msgs).verifyComplete();
  }
}
