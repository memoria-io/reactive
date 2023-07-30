package io.memoria.reactive.testsuite.eventsourcing;

import io.memoria.reactive.eventsourcing.Command;
import io.memoria.reactive.eventsourcing.Event;
import io.memoria.reactive.eventsourcing.State;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ESScenario<C extends Command, E extends Event, S extends State> {
  Flux<C> publishCommands();

  Flux<E> handleCommands();

  Mono<Boolean> verify(Flux<E> events);
}
