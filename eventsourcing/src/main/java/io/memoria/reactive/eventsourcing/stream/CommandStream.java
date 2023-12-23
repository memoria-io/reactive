package io.memoria.reactive.eventsourcing.stream;

import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.atom.eventsourcing.Command;
import io.memoria.reactive.core.stream.MsgStream;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface CommandStream {
  Mono<Command> pub(String topic, int partition, Command cmd);

  Flux<Command> sub(String topic, int partition);

  static CommandStream msgStream(MsgStream msgStream, TextTransformer transformer) {
    return new MsgCommandStream(msgStream, transformer);
  }

  static CommandStream inMemory() {
    return CommandStream.msgStream(MsgStream.inMemory(), new SerializableTransformer());
  }

  static CommandStream inMemory(int history) {
    return CommandStream.msgStream(MsgStream.inMemory(history), new SerializableTransformer());
  }
}

