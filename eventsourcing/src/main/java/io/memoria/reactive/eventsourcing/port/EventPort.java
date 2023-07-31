package io.memoria.reactive.eventsourcing.port;

import io.memoria.reactive.eventsourcing.Event;
import io.memoria.reactive.eventsourcing.StateId;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface EventPort<E extends Event> {
  Flux<E> events(String table, StateId stateId);

  Mono<Integer> append(String table, int seqIdx, E event);
}
