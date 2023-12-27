package io.memoria.reactive.eventsourcing.stream;

import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.EventId;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface EventRepo {

  Mono<Event> publish(Event event);

  Flux<Event> subscribe(int partition);

  Mono<Event> last(int partition);

  /**
   * @return subscribe until eventId (key) is matched
   */
  default Flux<Event> subUntil(int partition, EventId eventId) {
    return subscribe(partition).takeUntil(e -> e.meta().eventId().equals(eventId));
  }

  static EventRepo inMemory(int totalPartitions) {
    return new MemEventRepo(totalPartitions);
  }

  static EventRepo inMemory(int partition, int history) {
    return new MemEventRepo(partition, history);
  }
}

