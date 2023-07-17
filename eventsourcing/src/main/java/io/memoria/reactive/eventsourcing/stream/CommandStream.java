package io.memoria.reactive.eventsourcing.stream;

import io.memoria.reactive.eventsourcing.Command;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface CommandStream<C extends Command> {
  Mono<C> pub(String topic, int partition, C c);

  Flux<C> sub(String topic, int partition);

  static <C extends Command> CommandStream<C> inMemory() {
    return new MemCommandStream<>(Integer.MAX_VALUE);
  }

  static <C extends Command> CommandStream<C> inMemory(int history) {
    return new MemCommandStream<>(history);
  }
}

