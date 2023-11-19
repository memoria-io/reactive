package io.memoria.reactive.eventsourcing.stream;

import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.reactive.core.stream.MsgStream;
import io.memoria.reactive.eventsourcing.Command;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface CommandStream<C extends Command> {
  Mono<C> pub(String topic, int partition, C c);

  Flux<C> sub(String topic, int partition);

  static <C extends Command> CommandStream<C> msgStream(MsgStream msgStream,
                                                        Class<C> cClass,
                                                        TextTransformer transformer) {
    return new MsgCommandStream<>(msgStream, cClass, transformer);
  }

  static <C extends Command> CommandStream<C> inMemory(Class<C> cClass) {
    return CommandStream.msgStream(MsgStream.inMemory(), cClass, new SerializableTransformer());
  }

  static <C extends Command> CommandStream<C> inMemory(int history, Class<C> cClass) {
    return CommandStream.msgStream(MsgStream.inMemory(history), cClass, new SerializableTransformer());
  }
}

