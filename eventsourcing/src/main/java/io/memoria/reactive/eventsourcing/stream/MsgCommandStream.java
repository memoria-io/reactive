package io.memoria.reactive.eventsourcing.stream;

import io.memoria.atom.core.text.TextTransformer;
import io.memoria.atom.eventsourcing.Command;
import io.memoria.reactive.core.stream.Msg;
import io.memoria.reactive.core.stream.MsgStream;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static io.memoria.reactive.core.reactor.ReactorUtils.tryToMono;

class MsgCommandStream implements CommandStream {

  private final MsgStream msgStream;
  private final TextTransformer transformer;

  public MsgCommandStream(MsgStream msgStream, TextTransformer transformer) {
    this.msgStream = msgStream;
    this.transformer = transformer;
  }

  @Override
  public Mono<Command> pub(String topic, int partition, Command cmd) {
    return toMsg(cmd).flatMap(msg -> msgStream.pub(topic, partition, msg)).map(msg -> cmd);
  }

  @Override
  public Flux<Command> sub(String topic, int partition) {
    return msgStream.sub(topic, partition).concatMap(this::toCmd);
  }

  Mono<Msg> toMsg(Command cmd) {
    return tryToMono(() -> transformer.serialize(cmd)).map(value -> new Msg(cmd.meta().commandId().value(), value));
  }

  Mono<Command> toCmd(Msg msg) {
    return tryToMono(() -> transformer.deserialize(msg.value(), Command.class));
  }
}
