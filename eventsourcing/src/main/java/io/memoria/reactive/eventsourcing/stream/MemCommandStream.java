package io.memoria.reactive.eventsourcing.stream;

import io.memoria.atom.eventsourcing.Command;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Many;

public class MemCommandStream implements CommandStream {
  private final Many<Command> commands;

  public MemCommandStream() {
    this(Integer.MAX_VALUE);
  }

  public MemCommandStream(int historySize) {
    this.commands = Sinks.many().replay().limit(historySize);
  }

  @Override
  public Mono<Command> pub(Command command) {
    return Mono.fromRunnable(() -> commands.tryEmitNext(command).orThrow()).thenReturn(command);
  }

  @Override
  public Flux<Command> sub() {
    return this.commands.asFlux();
  }

  //  @Override
  //  public Mono<Msg> last(String topic, int partition) {
  //    return Mono.defer(() -> Mono.justOrEmpty(lastMsg.get(topic))).flatMap(tp -> Mono.justOrEmpty(tp.get(partition)));
  //  }
}
