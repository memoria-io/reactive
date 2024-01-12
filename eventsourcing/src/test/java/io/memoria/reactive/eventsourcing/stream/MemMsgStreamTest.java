package io.memoria.reactive.eventsourcing.stream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

@TestMethodOrder(OrderAnnotation.class)
class MemMsgStreamTest {
  private static final int ELEMENTS_SIZE = 1000;
  private static final String topic = "topic";
  private static final int partition = 0;
  private static final MsgStream msgStream = MsgStream.inMemory();

  @Test
  @Order(0)
  void empty() {
    StepVerifier.create(msgStream.last(topic, partition)).expectComplete().verify();
  }

  @Test
  @Order(1)
  void publish() {
    // Given
    var messages = Flux.range(0, ELEMENTS_SIZE).map(MemMsgStreamTest::createMessage);

    // Then
    StepVerifier.create(messages.concatMap(msgStream::pub)).expectNextCount(ELEMENTS_SIZE).verifyComplete();
  }

  @Test
  @Order(2)
  void subscribe() throws InterruptedException {
    // Given
    var idx = new AtomicInteger();
    var latch = new CountDownLatch(ELEMENTS_SIZE);

    // then
    msgStream.sub(topic, partition).doOnNext(msg -> {
      Assertions.assertThat(msg.key()).isEqualTo(idx.getAndIncrement() + "");
      latch.countDown();
    }).subscribe();
    latch.await();
  }

  @Test
  @Order(3)
  void subscribeUntil() throws InterruptedException {
    // Given
    var idx = new AtomicInteger();
    var latch = new CountDownLatch(ELEMENTS_SIZE);

    // then
    msgStream.subUntil(topic, partition, String.valueOf(ELEMENTS_SIZE)).doOnNext(msg -> {
      Assertions.assertThat(msg.key()).isEqualTo(idx.getAndIncrement() + "");
      latch.countDown();
    }).subscribe();
    latch.await();
  }

  @Test
  @Order(4)
  void last() {
    StepVerifier.create(msgStream.last(topic, partition).map(Msg::key))
                .expectNext(String.valueOf(ELEMENTS_SIZE - 1))
                .verifyComplete();
  }

  private static Msg createMessage(Integer i) {
    return new Msg(topic, partition, i, "hello");
  }
}
