package io.memoria.reactive.eventsourcing.stream;

import io.memoria.atom.core.text.TextTransformer;
import io.memoria.atom.eventsourcing.Event;
import io.memoria.reactive.core.stream.Msg;
import io.memoria.reactive.core.stream.MsgStream;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static io.memoria.reactive.core.reactor.ReactorUtils.tryToMono;

class MsgEventStream implements EventStream {

  private final MsgStream msgStream;
  private final TextTransformer transformer;

  public MsgEventStream(MsgStream msgStream, TextTransformer transformer) {
    this.msgStream = msgStream;
    this.transformer = transformer;
  }

  @Override
  public Mono<Event> last(String topic, int partition) {
    return msgStream.last(topic, partition).flatMap(this::toCmd);
  }

  @Override
  public Mono<Event> pub(String topic, int partition, Event event) {
    return toMsg(event).flatMap(msg -> msgStream.pub(topic, partition, msg)).map(msg -> event);
  }

  @Override
  public Flux<Event> sub(String topic, int partition) {
    return msgStream.sub(topic, partition).concatMap(this::toCmd);
  }

  Mono<Msg> toMsg(Event event) {
    return tryToMono(() -> transformer.serialize(event)).map(value -> new Msg(event.meta().commandId().value(), value));
  }

  Mono<Event> toCmd(Msg msg) {
    return tryToMono(() -> transformer.deserialize(msg.value(), Event.class));
  }
}
