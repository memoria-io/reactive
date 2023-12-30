package io.memoria.reactive.eventsourcing.stream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicInteger;

class MemMsgStreamTest {
  private static final int ELEMENTS_SIZE = 1000;
  private static final String topic = "topic";
  private static final int partition = 0;
  private final MsgStream msgStream = MsgStream.inMemory();

  @Test
  void publishAndSubscribe() {
    StepVerifier.create(msgStream.last(topic, partition)).expectComplete().verify();
    // Given
    var messages = Flux.range(0, ELEMENTS_SIZE).map(i -> new Msg(i, "hello"));

    // When
    StepVerifier.create(messages.concatMap(msg -> msgStream.pub(topic, partition, msg)))
                .expectNextCount(ELEMENTS_SIZE)
                .verifyComplete();

    // Then
    var idx = new AtomicInteger();
    msgStream.subUntil(topic, partition, ELEMENTS_SIZE + "").doOnNext(msg -> {
      Assertions.assertThat(msg.key()).isEqualTo(idx.getAndIncrement() + "");
    }).subscribe();
  }
}
