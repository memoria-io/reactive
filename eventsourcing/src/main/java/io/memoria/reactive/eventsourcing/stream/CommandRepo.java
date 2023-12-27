package io.memoria.reactive.eventsourcing.stream;

import io.memoria.atom.eventsourcing.Command;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface CommandRepo {

  Mono<Command> publish(Command cmd);

  Flux<Command> subscribe();

  static CommandRepo inMemory() {
    return new MemCommandRepo();
  }

  static CommandRepo inMemory(int history) {
    return new MemCommandRepo(history);
  }
}
