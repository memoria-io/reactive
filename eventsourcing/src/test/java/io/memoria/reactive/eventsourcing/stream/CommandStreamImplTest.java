package io.memoria.reactive.eventsourcing.stream;

import io.memoria.atom.core.id.Id;
import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.reactive.core.stream.ESMsgStream;
import io.memoria.reactive.eventsourcing.Command;
import io.memoria.reactive.eventsourcing.pipeline.PipelineRoute;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

class CommandStreamImplTest {
  private static final Duration timeout = Duration.ofSeconds(5);
  private static final int ELEMENTS_SIZE = 1000;
  private static final Id S0 = Id.of(0);
  private static final Id S1 = Id.of(1);
  private static final PipelineRoute firstRoute = createRoute(0);
  private static final PipelineRoute secondRoute = createRoute(1);
  private static final ESMsgStream esStream = ESMsgStream.inMemory();
  private static final CommandStream<SomeCommand> commandStream = getStream();

  @Test
  void publishAndSubscribe() {
    // Given
    var cmds = createMessages(S0).concatWith(createMessages(S1));

    // When
    StepVerifier.create(cmds.flatMap(c -> pub(firstRoute, c))).expectNextCount(ELEMENTS_SIZE * 2).verifyComplete();

    // Then
    var latch0 = new AtomicInteger();
    sub(firstRoute).take(ELEMENTS_SIZE).doOnNext(cmd -> {
      Assertions.assertThat(cmd.stateId()).isEqualTo(S0);
      Assertions.assertThat(cmd.partition(firstRoute.cmdTotalPubPartitions())).isEqualTo(firstRoute.cmdSubPartition());
      latch0.incrementAndGet();
    }).subscribe();
    Awaitility.await().atMost(timeout).until(() -> latch0.get() == ELEMENTS_SIZE);

    // And
    var latch1 = new AtomicInteger();
    sub(secondRoute).take(ELEMENTS_SIZE).doOnNext(cmd -> {
      Assertions.assertThat(cmd.stateId()).isEqualTo(S1);
      Assertions.assertThat(cmd.partition(firstRoute.cmdTotalPubPartitions())).isEqualTo(secondRoute.cmdSubPartition());
      latch1.incrementAndGet();
    }).subscribe();
    Awaitility.await().atMost(timeout).until(() -> latch1.get() == ELEMENTS_SIZE);
  }

  private static Mono<SomeCommand> pub(PipelineRoute route, SomeCommand cmd) {
    var partition = cmd.partition(route.cmdTotalPubPartitions());
    return commandStream.pub(route.cmdTopic(), partition, cmd);
  }

  private static Flux<SomeCommand> sub(PipelineRoute route) {
    return commandStream.sub(route.cmdTopic(), route.cmdSubPartition());
  }

  private static CommandStream<SomeCommand> getStream() {
    return CommandStream.create(esStream, new SerializableTransformer(), SomeCommand.class);
  }

  private static PipelineRoute createRoute(int partition) {
    return new PipelineRoute("command_topic", partition, 2, "events_topic", partition);
  }

  private static Flux<SomeCommand> createMessages(Id stateId) {
    return Flux.range(0, ELEMENTS_SIZE).map(i -> new SomeCommand(Id.of(i), stateId, Id.of(i)));
  }

  private record SomeCommand(Id eventId, Id stateId, Id commandId) implements Command {
    @Override
    public long timestamp() {
      return 0;
    }
  }
}
