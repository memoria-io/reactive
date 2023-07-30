package io.memoria.reactive.testsuite.eventsourcing;

import io.memoria.reactive.eventsourcing.Event;
import reactor.core.publisher.Flux;

public interface EventStreamScenario<E extends Event> {
  Flux<E> publish();

  Flux<E> subscribe();
}
