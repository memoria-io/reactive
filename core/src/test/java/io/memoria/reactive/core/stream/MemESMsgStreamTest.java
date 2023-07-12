package io.memoria.reactive.core.stream;

import io.memoria.atom.core.id.Id;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

class MemESMsgStreamTest {
  private static final Duration timeout = Duration.ofSeconds(5);
  private static final int ELEMENTS_SIZE = 1000;
  private static final String topic = "some_topic";
  private static final Id S0 = Id.of(0);
  private static final Id S1 = Id.of(1);
  private static final int TOTAL_PARTITIONS = 2;

  private final ESMsgStream stream = new MemESMsgStream();

  @Test
  void publishAndSubscribe() {
    // Given
    var msgs = createMessages(S0).concatWith(createMessages(S1));
    var s0LastMessage = createMessage(S0, ELEMENTS_SIZE - 1);
    var s1LastMessage = createMessage(S0, ELEMENTS_SIZE - 1);

    // When
    StepVerifier.create(msgs.flatMap(stream::pub)).expectNextCount(ELEMENTS_SIZE * 2).verifyComplete();

    // Then
    var latch0 = new AtomicInteger();
    stream.sub(topic, 0).take(ELEMENTS_SIZE).index().doOnNext(tup -> {
      Assertions.assertThat(tup.getT2().key()).isEqualTo(String.valueOf(tup.getT1().intValue()));
      Assertions.assertThat(tup.getT2().partition()).isZero();
      latch0.incrementAndGet();
    }).subscribe();
    Awaitility.await().atMost(timeout).until(() -> latch0.get() == ELEMENTS_SIZE);

    // And
    var latch1 = new AtomicInteger();
    stream.sub(topic, 1).take(ELEMENTS_SIZE).index().doOnNext(tup -> {
      Assertions.assertThat(tup.getT2().key()).isEqualTo(String.valueOf(tup.getT1().intValue()));
      Assertions.assertThat(tup.getT2().partition()).isEqualTo(1);
      latch1.incrementAndGet();
    }).subscribe();
    Awaitility.await().atMost(timeout).until(() -> latch1.get() == ELEMENTS_SIZE);

    // And
    StepVerifier.create(stream.lastKey(topic, 0)).expectNext(s0LastMessage.key()).verifyComplete();
    StepVerifier.create(stream.lastKey(topic, 1)).expectNext(s1LastMessage.key()).verifyComplete();
  }

  private Flux<ESMsg> createMessages(Id stateId) {
    return Flux.range(0, ELEMENTS_SIZE).map(i -> createMessage(stateId, i));
  }

  private static ESMsg createMessage(Id stateId, Integer i) {
    return new ESMsg(topic, getPartition(stateId), String.valueOf(i), "hello");
  }

  private static int getPartition(Id stateId) {
    return Integer.parseInt(stateId.value()) % TOTAL_PARTITIONS;
  }
}
