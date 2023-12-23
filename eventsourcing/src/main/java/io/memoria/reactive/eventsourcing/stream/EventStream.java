package io.memoria.reactive.eventsourcing.stream;

import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.EventId;
import io.memoria.reactive.core.stream.MsgStream;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface EventStream {

  Mono<Event> pub(String topic, int partition, Event event);

  Flux<Event> sub(String topic, int partition);

  Mono<Event> last(String topic, int partition);

  /**
   * @return subscribe until eventId (key) is matched
   */
  default Flux<Event> subUntil(String topic, int partition, EventId eventId) {
    return sub(topic, partition).takeUntil(e -> e.meta().eventId().equals(eventId));
  }

  static EventStream msgStream(MsgStream msgStream, TextTransformer transformer) {
    return new MsgEventStream(msgStream, transformer);
  }

  static EventStream inMemory() {
    return EventStream.msgStream(MsgStream.inMemory(), new SerializableTransformer());
  }

  static EventStream inMemory(int history) {
    return EventStream.msgStream(MsgStream.inMemory(history), new SerializableTransformer());
  }
}

