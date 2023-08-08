package io.memoria.reactive.eventsourcing.stream;

import io.memoria.atom.eventsourcing.CommandId;
import io.memoria.atom.eventsourcing.EventMeta;
import io.memoria.atom.eventsourcing.StateId;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

class EventStreamTest {
  private static final Duration timeout = Duration.ofSeconds(5);
  private static final int ELEMENTS_SIZE = 1000;
  private static final StateId s0 = StateId.of(0L);
  private static final String topic = "commands";
  private static final int totalPartitions = 1;
  private final EventStream<SomeEvent> stream = EventStream.inMemory(SomeEvent.class);

  @Test
  void publishAndSubscribe() {
    // Given
    var cmds = Flux.range(0, ELEMENTS_SIZE).map(i -> new SomeEvent(new EventMeta(CommandId.of(), s0)));

    // When
    StepVerifier.create(cmds.flatMap(c -> stream.pub(topic, 0, c))).expectNextCount(ELEMENTS_SIZE).verifyComplete();

    // Then
    var latch0 = new AtomicInteger();
    stream.sub(topic, 0).take(ELEMENTS_SIZE).doOnNext(event -> {
      verify(event);
      latch0.incrementAndGet();
    }).subscribe();
    Awaitility.await().atMost(timeout).until(() -> latch0.get() == ELEMENTS_SIZE);
  }

  private static void verify(SomeEvent event) {
    Assertions.assertThat(event.meta().stateId()).isEqualTo(s0);
    Assertions.assertThat(event.meta().partition(totalPartitions)).isZero();
  }
}
