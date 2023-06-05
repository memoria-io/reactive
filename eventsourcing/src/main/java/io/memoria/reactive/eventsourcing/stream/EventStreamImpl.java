package io.memoria.reactive.eventsourcing.stream;

import io.memoria.atom.core.text.TextTransformer;
import io.memoria.reactive.core.stream.ESMsg;
import io.memoria.reactive.core.stream.ESMsgStream;
import io.memoria.reactive.core.vavr.ReactorVavrUtils;
import io.memoria.reactive.eventsourcing.Event;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class EventStreamImpl<E extends Event> implements EventStream<E> {
  private final ESMsgStream esMsgStream;
  private final TextTransformer transformer;
  private final Class<E> cClass;

  EventStreamImpl(ESMsgStream esMsgStream, TextTransformer transformer, Class<E> cClass) {
    this.esMsgStream = esMsgStream;
    this.transformer = transformer;
    this.cClass = cClass;
  }

  public Mono<E> pub(String topic, int partition, E e) {
    return ReactorVavrUtils.tryToMono(() -> transformer.serialize(e))
                           .flatMap(cStr -> pubMsg(topic, partition, e, cStr))
                           .map(id -> e);
  }

  public Flux<E> sub(String topic, int partition) {
    return esMsgStream.sub(topic, partition)
                      .flatMap(msg -> ReactorVavrUtils.tryToMono(() -> transformer.deserialize(msg.value(), cClass)));

  }

  private Mono<ESMsg> pubMsg(String topic, int partition, E e, String cStr) {
    return esMsgStream.pub(new ESMsg(topic, partition, e.commandId().value(), cStr));
  }
}
