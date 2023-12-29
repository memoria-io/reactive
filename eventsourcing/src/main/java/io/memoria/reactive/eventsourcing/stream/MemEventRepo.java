package io.memoria.reactive.eventsourcing.stream;

import io.memoria.atom.eventsourcing.Event;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Many;

import java.util.concurrent.atomic.AtomicReference;

public class MemEventRepo implements EventRepo {
  private final Many<Event> events;
  private final AtomicReference<Event> lastEvent;

  public MemEventRepo() {
    this(Integer.MAX_VALUE);
  }

  public MemEventRepo(int historySize) {
    this.events = Sinks.many().replay().limit(historySize);
    this.lastEvent = new AtomicReference<>();
  }

  @Override
  public Mono<Event> pub(Event event) {
    return Mono.fromRunnable(() -> {
      lastEvent.set(event);
      events.tryEmitNext(event).orThrow();
    }).thenReturn(event);
  }

  @Override
  public Flux<Event> sub() {
    return events.asFlux();
  }

  @Override
  public Mono<Event> last() {
    return Mono.justOrEmpty(this.lastEvent.get());
  }
}
