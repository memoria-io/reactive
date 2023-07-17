package io.memoria.reactive.eventsourcing.stream;

import io.memoria.atom.core.id.Id;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

class CommandStreamTest {
  private static final Duration timeout = Duration.ofSeconds(5);
  private static final int ELEMENTS_SIZE = 1000;
  private static final Id S0 = Id.of(0);
  private static final String topic = "commands";
  private static final int totalPartitions = 1;
  private final CommandStream<SomeCommand> stream = CommandStream.inMemory();

  @Test
  void publishAndSubscribe() {
    // Given
    var cmds = Flux.range(0, ELEMENTS_SIZE).map(i -> new SomeCommand(Id.of(i), S0, Id.of(i)));

    // When
    StepVerifier.create(cmds.flatMap(c -> stream.pub(topic, 0, c))).expectNextCount(ELEMENTS_SIZE).verifyComplete();

    // Then
    var latch0 = new AtomicInteger();
    stream.sub(topic, 0).take(ELEMENTS_SIZE).doOnNext(cmd -> {
      verify(cmd);
      latch0.incrementAndGet();
    }).subscribe();
    Awaitility.await().atMost(timeout).until(() -> latch0.get() == ELEMENTS_SIZE);
  }

  private static void verify(SomeCommand cmd) {
    Assertions.assertThat(cmd.stateId()).isEqualTo(S0);
    Assertions.assertThat(cmd.partition(totalPartitions)).isEqualTo(0);
  }
}
