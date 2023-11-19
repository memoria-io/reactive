package io.memoria.reactive.eventsourcing.stream;

import io.memoria.atom.core.text.TextTransformer;
import io.memoria.reactive.core.stream.Msg;
import io.memoria.reactive.core.stream.MsgStream;
import io.memoria.reactive.eventsourcing.Event;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static io.memoria.reactive.core.reactor.ReactorUtils.tryToMono;

class MsgEventStream<E extends Event> implements EventStream<E> {

  private final MsgStream msgStream;
  private final Class<E> cClass;
  private final TextTransformer transformer;

  public MsgEventStream(MsgStream msgStream, Class<E> cClass, TextTransformer transformer) {
    this.msgStream = msgStream;
    this.cClass = cClass;
    this.transformer = transformer;
  }

  @Override
  public Mono<E> last(String topic, int partition) {
    return msgStream.last(topic, partition).flatMap(this::toCmd);
  }

  @Override
  public Mono<E> pub(String topic, int partition, E cmd) {
    return toMsg(cmd).flatMap(msg -> msgStream.pub(topic, partition, msg)).map(msg -> cmd);
  }

  @Override
  public Flux<E> sub(String topic, int partition) {
    return msgStream.sub(topic, partition).concatMap(this::toCmd);
  }

  Mono<Msg> toMsg(E cmd) {
    return tryToMono(() -> transformer.serialize(cmd)).map(value -> new Msg(cmd.meta().commandId().value(), value));
  }

  Mono<E> toCmd(Msg msg) {
    return tryToMono(() -> transformer.deserialize(msg.value(), cClass));
  }
}
