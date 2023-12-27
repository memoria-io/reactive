package io.memoria.reactive.eventsourcing.stream;

import io.memoria.atom.eventsourcing.Command;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface CommandStream {

  Mono<Command> pub(Command cmd);

  Flux<Command> sub();

  static CommandStream inMemory() {
    return new MemCommandStream();
  }

  static CommandStream inMemory(int history) {
    return new MemCommandStream(history);
  }
}

