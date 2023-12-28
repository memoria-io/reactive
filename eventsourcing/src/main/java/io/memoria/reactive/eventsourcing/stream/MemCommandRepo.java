package io.memoria.reactive.eventsourcing.stream;

import io.memoria.atom.eventsourcing.Command;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Many;

class MemCommandRepo implements CommandRepo {
  private final Many<Command> commands;

  public MemCommandRepo() {
    this(Integer.MAX_VALUE);
  }

  public MemCommandRepo(int historySize) {
    this.commands = Sinks.many().replay().limit(historySize);
  }

  @Override
  public Mono<Command> publish(Command command) {
    return Mono.fromRunnable(() -> commands.tryEmitNext(command).orThrow()).thenReturn(command);
  }

  @Override
  public Flux<Command> subscribe() {
    return this.commands.asFlux();
  }
}
