package io.memoria.reactive.testsuite.eventsourcing.banking.pipeline.partition;

import io.memoria.atom.eventsourcing.Command;
import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.State;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface PartitionScenario<C extends Command, E extends Event> {
  int expectedCommandsCount();

  int expectedEventsCount();

  Flux<C> publishCommands();

  Flux<E> handleCommands();

  Mono<Boolean> verify();
}
