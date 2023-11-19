package io.memoria.reactive.eventsourcing.stream;

import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.EventId;
import io.memoria.reactive.core.stream.MsgStream;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface EventStream<E extends Event> {

  Mono<E> pub(String topic, int partition, E e);

  Flux<E> sub(String topic, int partition);

  Mono<E> last(String topic, int partition);

  /**
   * @return subscribe until eventId (key) is matched
   */
  default Flux<E> subUntil(String topic, int partition, EventId eventId) {
    return sub(topic, partition).takeUntil(e -> e.meta().eventId().equals(eventId));
  }

  static <E extends Event> EventStream<E> msgStream(MsgStream msgStream, Class<E> cClass, TextTransformer transformer) {
    return new MsgEventStream<>(msgStream, cClass, transformer);
  }

  static <E extends Event> EventStream<E> inMemory(Class<E> cClass) {
    return EventStream.msgStream(MsgStream.inMemory(), cClass, new SerializableTransformer());
  }

  static <E extends Event> EventStream<E> inMemory(int history, Class<E> cClass) {
    return EventStream.msgStream(MsgStream.inMemory(history), cClass, new SerializableTransformer());
  }
}

