package io.memoria.reactive.eventsourcing.stream;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.Event;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface EventStream<E extends Event> {
  Mono<Id> last(String topic, int partition);

  Mono<E> pub(String topic, int partition, E e);

  Flux<E> sub(String topic, int partition);

  /**
   * @return subscribe until eventId (key) is matched
   */
  default Flux<E> subUntil(String topic, int partition, Id eventId) {
    return sub(topic, partition).takeUntil(e -> e.eventId().equals(eventId));
  }

  static <E extends Event> EventStream<E> inMemory() {
    return new MemEventStream<>(Integer.MAX_VALUE);
  }

  static <E extends Event> EventStream<E> inMemory(int history) {
    return new MemEventStream<>(history);
  }
}

