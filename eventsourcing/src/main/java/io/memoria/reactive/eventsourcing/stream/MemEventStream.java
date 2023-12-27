package io.memoria.reactive.eventsourcing.stream;

import io.memoria.atom.eventsourcing.Event;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Many;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class MemEventStream implements EventStream {
  private final int totalPartitions;
  private final Map<Integer, Many<Event>> events;
  private final Map<Integer, AtomicReference<Event>> lastEvent;

  public MemEventStream(int totalPartitions) {
    this(totalPartitions, Integer.MAX_VALUE);
  }

  public MemEventStream(int totalPartitions, int historySize) {
    this.totalPartitions = totalPartitions;
    this.events = new ConcurrentHashMap<>();
    this.lastEvent = new ConcurrentHashMap<>();
    for (int i = 0; i < totalPartitions; i++) {
      this.events.put(i, Sinks.many().replay().limit(historySize));
      this.lastEvent.put(i, new AtomicReference<>());
    }
  }

  @Override
  public Mono<Event> pub(Event event) {
    var partition = event.partition(totalPartitions);
    return Mono.fromRunnable(() -> {
      lastEvent.computeIfPresent(partition, (k, v) -> {
        v.set(event);
        return v;
      });
      events.computeIfPresent(partition, (k, v) -> {
        v.tryEmitNext(event).orThrow();
        return v;
      });
    }).thenReturn(event);
  }

  @Override
  public Flux<Event> sub(int partition) {
    return events.get(partition).asFlux();
  }

  @Override
  public Mono<Event> last(int partition) {
    return Mono.defer(() -> Mono.justOrEmpty(lastEvent.get(partition))).map(AtomicReference::get);
  }
}
