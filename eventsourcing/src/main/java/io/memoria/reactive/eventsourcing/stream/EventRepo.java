package io.memoria.reactive.eventsourcing.stream;

import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.EventId;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface EventRepo {

  Mono<Event> publish(Event event);

  Flux<Event> subscribe();

  Mono<Event> last();

  /**
   * @return subscribe until eventId (key) is matched
   */
  default Flux<Event> subUntil(EventId eventId) {
    return subscribe().takeUntil(e -> e.meta().eventId().equals(eventId));
  }

  static EventRepo inMemory() {
    return new MemEventRepo();
  }

  static EventRepo inMemory(int history) {
    return new MemEventRepo(history);
  }
}

