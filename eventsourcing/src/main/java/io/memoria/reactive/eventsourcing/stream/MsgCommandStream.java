package io.memoria.reactive.eventsourcing.stream;

import io.memoria.atom.core.text.TextTransformer;
import io.memoria.reactive.core.stream.Msg;
import io.memoria.reactive.core.stream.MsgStream;
import io.memoria.reactive.eventsourcing.Command;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static io.memoria.reactive.core.reactor.ReactorUtils.tryToMono;

class MsgCommandStream<C extends Command> implements CommandStream<C> {

  private final MsgStream msgStream;
  private final Class<C> cClass;
  private final TextTransformer transformer;

  public MsgCommandStream(MsgStream msgStream, Class<C> cClass, TextTransformer transformer) {
    this.msgStream = msgStream;
    this.cClass = cClass;
    this.transformer = transformer;
  }

  @Override
  public Mono<C> pub(String topic, int partition, C cmd) {
    return toMsg(cmd).flatMap(msg -> msgStream.pub(topic, partition, msg)).map(msg -> cmd);
  }

  @Override
  public Flux<C> sub(String topic, int partition) {
    return msgStream.sub(topic, partition).concatMap(this::toCmd);
  }

  Mono<Msg> toMsg(C cmd) {
    return tryToMono(() -> transformer.serialize(cmd)).map(value -> new Msg(cmd.meta().commandId().value(), value));
  }

  Mono<C> toCmd(Msg msg) {
    return tryToMono(() -> transformer.deserialize(msg.value(), cClass));
  }
}
