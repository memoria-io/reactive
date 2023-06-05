package io.memoria.reactive.eventsourcing.stream;

import io.memoria.atom.core.id.Id;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.reactive.core.stream.ESMsgStream;
import io.memoria.reactive.eventsourcing.Event;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface EventStream<E extends Event> {
  Mono<E> pub(String topic, int partition, E e);

  /**
   * @return Infinite subscription
   */
  Flux<E> sub(String topic, int partition);

  /**
   * @return subscribe until eventId (key) is matched
   */
  default Flux<E> subUntil(String topic, int partition, Id eventId) {
    return sub(topic, partition).takeUntil(e -> e.eventId().equals(eventId));
  }

  static <E extends Event> EventStream<E> create(ESMsgStream esMsgStream,
                                                 TextTransformer transformer,
                                                 Class<E> eClass) {
    return new EventStreamImpl<>(esMsgStream, transformer, eClass);
  }
}
