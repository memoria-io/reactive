package io.memoria.reactive.eventsourcing.stream;

import io.memoria.atom.eventsourcing.Command;
import io.memoria.atom.eventsourcing.CommandId;
import io.memoria.atom.eventsourcing.CommandMeta;
import io.memoria.atom.eventsourcing.StateId;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

class CommandRepoTest {
  private static final Duration timeout = Duration.ofSeconds(5);
  private static final int ELEMENTS_SIZE = 1000;
  private static final StateId s0 = StateId.of(0);
  private static final int totalPartitions = 1;
  private final CommandRepo stream = CommandRepo.inMemory();

  @Test
  void publishAndSubscribe() {
    // Given
    var cmds = Flux.range(0, ELEMENTS_SIZE)
                   .map(i -> new SomeCommand(new CommandMeta(CommandId.of(UUID.randomUUID()), s0)));

    // When
    StepVerifier.create(cmds.flatMap(stream::pub)).expectNextCount(ELEMENTS_SIZE).verifyComplete();

    // Then
    var latch0 = new AtomicInteger();
    stream.sub().take(ELEMENTS_SIZE).doOnNext(cmd -> {
      verify(cmd);
      latch0.incrementAndGet();
    }).subscribe();
    Awaitility.await().atMost(timeout).until(() -> latch0.get() == ELEMENTS_SIZE);
  }

  private static void verify(Command cmd) {
    Assertions.assertThat(cmd.meta().stateId()).isEqualTo(s0);
    Assertions.assertThat(cmd.meta().partition(totalPartitions)).isZero();
  }
}
