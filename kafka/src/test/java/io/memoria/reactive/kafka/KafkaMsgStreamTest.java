package io.memoria.reactive.kafka;

import io.memoria.reactive.core.msg.stream.MsgStream;
import io.memoria.reactive.testsuite.TestsuiteDefaults;
import io.vavr.collection.List;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;

import static io.memoria.reactive.testsuite.TestsuiteDefaults.MSG_COUNT;
import static io.memoria.reactive.testsuite.TestsuiteDefaults.TIMEOUT;

@TestMethodOrder(OrderAnnotation.class)
class KafkaMsgStreamTest {
  private static final String topic = TestsuiteDefaults.topicName("messages");
  private static final int partition = 0;
  private static final MsgStream repo;

  static {
    repo = new KafkaMsgStream(TestUtils.producerConfigs(), TestUtils.consumerConfigs(), Duration.ofMillis(500));
  }

  @Test
  @Order(0)
  void lastKey() {
    StepVerifier.create(repo.last(topic, partition)).expectComplete().verify();
  }

  @Test
  @Order(1)
  void publish() {
    // Given
    var msgs = List.range(0, MSG_COUNT).map(TestsuiteDefaults::createEsMsg);
    // When
    var pub = Flux.fromIterable(msgs).concatMap(msg -> repo.pub(topic, partition, msg));
    // Then
    var now = System.currentTimeMillis();

    StepVerifier.create(pub).expectNextCount(MSG_COUNT).verifyComplete();

    long totalElapsed = System.currentTimeMillis() - now;
    System.out.printf("Finished processing %d events, in %d millis %n", MSG_COUNT, totalElapsed);
    System.out.printf("Average %f events per second %n", MSG_COUNT / (totalElapsed / 1000d));
    //    StepVerifier.create(repo.last(topic, partition)).expectNext(msgs.last()).verifyComplete();
  }

  @Test
  @Order(2)
  void subscribe() {
    var sub = repo.sub(topic, partition);

    var now = System.currentTimeMillis();

    StepVerifier.create(sub).expectNextCount(MSG_COUNT).expectTimeout(TIMEOUT).verify();

    long totalElapsed = System.currentTimeMillis() - now;
    System.out.printf("Finished processing %d events, in %d millis %n", MSG_COUNT, totalElapsed);
    System.out.printf("Average %f events per second %n", MSG_COUNT / (totalElapsed / 1000d));
    //    StepVerifier.create(sub).expectNextSequence(msgs).expectTimeout(TIMEOUT).verify();
  }
}
